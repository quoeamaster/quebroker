package quebroker

import (
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
)

// github.com/jinzhu/configor

const paramEnvTomlConfigPath = "QUE_CONFIG_TOML"
const brokerConfigToml = "quebroker.toml"

// Broker - representing a broker instance
type Broker struct {
	ID                         string   // actual UUID of the broker (however broker_id is SET automatically ONLY for the 1st time)
	Name                       string   // name of broker for human readability
	BootstrapServersByHostname []string // the servers to handshake when joining or creating the cluster

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
	fmt.Println("existing file path", _tomlPath)
	fmt.Println(configor.Config{Debug: true})
	// TODO: test the config2Broker

	return
}

func decodeTomlConfig2BrokerStruct() {

}

// isFileExists - check whether the given path (and its optional paths) exists or not
func isFileExists(path string, paths ...string) (exists bool, configPath string) {
	_path := path
	if paths != nil {
		if strings.Compare(_path, "") > 0 {
			_path = fmt.Sprintf("%v/%v", _path, strings.Join(paths, ""))
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
