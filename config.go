// Licensed to quoeamaster@gmail.com under one or more contributor
// license agreements. See the LICENSE file distributed with
// this work for additional information regarding copyright
// ownership. quoeamaster@gmail.com licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package quebroker

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/jinzhu/configor"
)

// CreateFolders - method to create folder(s) such as Path.Data, Path.Log
func (b *Broker) CreateFolders() (err error) {
	_oldUMask := syscall.Umask(0)

	var _folderRight os.FileMode
	_folderRight = 0755 // for writes... (so creation of file need 'X', hence owner should be RWX = 7, others... usually for R+X = 5)

	// [Path] Data
	_exists, _ := isFileExists(b.Path.Data)
	if !_exists {
		err = os.MkdirAll(b.Path.Data, _folderRight)
		if err != nil {
			return
		}
	}
	// [Path] Log
	_exists, _ = isFileExists(b.Path.Log)
	if !_exists {
		err = os.MkdirAll(b.Path.Log, _folderRight)
		if err != nil {
			return
		}
	}
	// TODO: update folder creation if more configs were added
	syscall.Umask(_oldUMask)

	return
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
		err = os.MkdirAll(_homePath, 0755) // 755 or 644 (RWX => 421)
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
	//fmt.Println(_seed)
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
	// create folder(s) e.g. path.data, path.log
	err = instance.CreateFolders()
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
	// replace ENV VARS from the config strings (needs to update manually....)
	// DOC: study on using reflection ? (but performance penalty)
	_re, err := regexp.Compile(`\$\{[a-z|A-Z|_|-]+\}`)
	if err != nil {
		return
	}
	instance.Name = replaceEnvVarValuesToString(instance.Name, _re)
	instance.Cluster.Name = replaceEnvVarValuesToString(instance.Cluster.Name, _re)
	instance.Network.HostName = replaceEnvVarValuesToString(instance.Network.HostName, _re)
	instance.Network.Port, err = strconv.Atoi(replaceEnvVarValuesToString(strconv.Itoa(instance.Network.Port), _re))
	if err != nil {
		return
	}
	instance.Path.Data = replaceEnvVarValuesToString(instance.Path.Data, _re)
	instance.Path.Log = replaceEnvVarValuesToString(instance.Path.Log, _re)

	// TODO: update the setters when new config values are available

	return
}

// replaceEnvVarValuesToString - substitute the env variable values to the given string
func replaceEnvVarValuesToString(value string, re *regexp.Regexp) (finalValue string) {
	finalValue = value

	_params := re.FindAllString(finalValue, -1)
	for _, _param := range _params {
		// strip the heading "${" and trailing "}"
		_envParam := (_param[:len(_param)-1])[2:]

		// handle the env replacement
		switch _envParam {
		case paramEnvHomeDir:
			_replace, err := os.UserHomeDir()
			if err != nil {
				return
			}
			_replace = fmt.Sprintf("%v%v%v", _replace, string(os.PathSeparator), brokerHomeDir)
			finalValue = strings.Replace(finalValue, _param, _replace, 1)

		default:
			_replace := os.Getenv(_envParam)
			finalValue = strings.Replace(finalValue, _param, _replace, 1)
		}
	}
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
	//fmt.Println("toml config path", _path, "*")
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
