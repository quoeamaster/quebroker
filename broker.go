package quebroker

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/jinzhu/configor"
)

// github.com/jinzhu/configor

const paramEnvTomlConfigPath = "QUE_CONFIG_TOML"
const paramEnvHomeDir = "HOME"
const brokerConfigToml = "quebroker.toml"
const brokerHomeDir = ".quebroker"
const brokerIDFile = ".broker.id"
const brokerClusterIDFile = ".cluster.id"

// Broker - representing a broker instance
type Broker struct {
	// actual UUID of the broker (however broker_id is SET automatically ONLY for the 1st time)
	ID string `default:""`
	// name of broker for human readability
	Name string `required:"true"`
	// the servers to handshake when joining or creating the cluster
	BootstrapServers []string

	Cluster struct {
		ID   string // actual UUID of the cluster (should also be calculated for the 1st time using UUID or murmur3 hash etc)
		Name string // name of cluster for human readability
	}

	Path struct {
		Data string // where to store the queue messages on this instance
		Log  string // where to store the broker's logs
	}

	Network struct {
		HostName string // actual hostname or IP for connection
		Port     int    // port number for this host
	}
}

/// String - description of a Broker
func (b *Broker) String() string {
	var _b strings.Builder

	_b.WriteString("broker instance:\n")
	_b.WriteString(fmt.Sprintf("  id [%v], name [%v]\n", b.ID, b.Name))
	_b.WriteString(fmt.Sprintf("  cluster.id [%v], cluster.name [%v]\n", b.Cluster.ID, b.Cluster.Name))
	_b.WriteString(fmt.Sprintf("  boostrap.server.list [%v] of size %v\n", b.BootstrapServers, len(b.BootstrapServers)))
	_b.WriteString(fmt.Sprintf("  network.hostname [%v], network.port [%v]\n", b.Network.HostName, b.Network.Port))
	_b.WriteString(fmt.Sprintf("  path.data [%v], path.log [%v]", b.Path.Data, b.Path.Log))

	return _b.String()
}

// PopulateBrokerIDs - populate / create corresponding Broker ID(s) (e.g. broker.id, cluster.id)
func (b *Broker) PopulateBrokerIDs() (err error) {
	// "go tool dist list" - list all valid OS architecture
	// a. find HOME directory
	_home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	// b. {home}/.quebroker exists?
	_exists, _homePath := isFileExists(_home, brokerHomeDir)
	if !_exists {
		_umaskOld := syscall.Umask(0) // resetting the umask on creating file's permission
		// c. create home folder .quebroker
		_homePath = fmt.Sprintf("%v%v%v", _home, string(os.PathSeparator), brokerHomeDir)
		err = os.MkdirAll(_homePath, 0755) // 755 or 644 or 655 (RW- RW- RW-)
		if err != nil {
			return
		}
		// d. generate id
		err = b.generateIDs()
		if err != nil {
			return
		}
		// e. create the id files under the home folder (.quebroker)
		err = b.createIDFile(_homePath, brokerIDFile, b.ID)
		if err != nil {
			return
		}
		err = b.createIDFile(_homePath, brokerClusterIDFile, b.Cluster.ID)
		if err != nil {
			return
		}

		// reset the umask
		syscall.Umask(_umaskOld)
	} else {
		// load the .broker.id and .cluster.id file values back to the Broker instance
		b.ID, err = b.readIDFromFile(_homePath, brokerIDFile)
		if err != nil {
			return
		}
		b.Cluster.ID, err = b.readIDFromFile(_homePath, brokerClusterIDFile)
		if err != nil {
			return
		}
	}
	return
}

// generateIDs - method to generate broker.id and cluster.id (usually for the 1st time)
func (b *Broker) generateIDs() (err error) {
	// a. get hostname and concat with home.dir, then trim to first 16 characters
	_hostName, err := os.Hostname()
	if err != nil {
		return
	}
	_homeDir, err := os.UserHomeDir()
	if err != nil {
		return
	}
	_homeDir = strings.ReplaceAll(_homeDir, string(os.PathSeparator), "_")
	_seed := fmt.Sprintf("%v_%v", _hostName, _homeDir)[:16] // extract the 1st 16 chars from the combined host+homeDir value

	// b. generate UUID for broker.id
	_id, err := uuid.FromBytes([]byte(_seed))
	if err != nil {
		// fmt.Println("err", err)
		return
	}
	b.ID = _id.String()

	// c. generate UUID for cluster.id
	_seed = fmt.Sprintf("%16s", b.Cluster.Name)
	fmt.Println(_seed)
	_cid, err := uuid.FromBytes([]byte(_seed))
	if err != nil {
		// fmt.Println("err", err)
		return
	}
	b.Cluster.ID = _cid.String()

	return
}

// createIDFile - create a file storing the content (ID)
func (b *Broker) createIDFile(home string, filepath string, id string) (err error) {
	_filepath := fmt.Sprintf("%v%v%v", home, string(os.PathSeparator), filepath)
	err = ioutil.WriteFile(_filepath, []byte(id), 0644)

	return
}

// readIDFromFile - read id from the given file path
func (b *Broker) readIDFromFile(home, filepath string) (id string, err error) {
	_filepath := fmt.Sprintf("%v%v%v", home, string(os.PathSeparator), filepath)
	_bytes, err := ioutil.ReadFile(_filepath)
	if err != nil {
		return
	}
	id = string(_bytes)
	return
}

// BrokerInstanceFromTomlConfig - create a broker instance based on the toml config
func BrokerInstanceFromTomlConfig() (instance *Broker, err error) {
	// get the toml config file location
	// a) Env var or // b) local path
	_tomlPath := os.Getenv(paramEnvTomlConfigPath)
	_exists, _tomlPath := isFileExists(_tomlPath, brokerConfigToml)

	if !_exists {
		err = fmt.Errorf(`
		config file [%v] NOT exists! Can also check whether the environment variable [%v] has been setup correctly`,
			brokerConfigToml, paramEnvTomlConfigPath)
		return
	}
	// load file contents and populate into the Broker struct
	//fmt.Println("existing file path", _tomlPath)

	// test the config2Broker
	instance, err = decodeTomlConfig2BrokerStruct(_tomlPath)
	if err != nil {
		return
	}
	// populate id(s)
	err = instance.PopulateBrokerIDs()
	if err != nil {
		return
	}

	return
}

// decodeTomlConfig2BrokerStruct - load the toml' content into a Broker instance
func decodeTomlConfig2BrokerStruct(filepath string) (instance *Broker, err error) {
	instance = new(Broker)
	//err = configor.New(&configor.Config{Debug: true}).Load(instance, filepath)
	err = configor.Load(instance, filepath)
	// TODO: replace ENV VARS from the config strings

	return
}

// isFileExists - check whether the given path (and its optional paths) exists or not
func isFileExists(path string, paths ...string) (exists bool, configPath string) {
	_path := path
	if paths != nil {
		if strings.Compare(_path, "") > 0 {
			_path = fmt.Sprintf("%v%v%v", _path, string(os.PathSeparator), strings.Join(paths, ""))
		} else {
			_path = strings.Join(paths, "")
		}
	}
	fmt.Println("toml config path", _path, "*")
	_, err := os.Stat(_path)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	// all good
	exists = true
	configPath = _path
	return
}
