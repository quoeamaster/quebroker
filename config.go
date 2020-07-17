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
	"github.com/quoeamaster/quebroker/util"
)

// CreateFolders - method to create folder(s) such as Path.Data, Path.Log
func (b *Broker) createFolders() (err error) {
	_oldUMask := syscall.Umask(0)

	var _folderRight os.FileMode
	_folderRight = 0755 // for writes... (so creation of file need 'X', hence owner should be RWX = 7, others... usually for R+X = 5)

	// [Path] Data
	log.Tracef("[createFolders] *** data: %v log: %v", b.Path.Data, b.Path.Log)
	_exists, _ := util.IsFileExists(b.Path.Data)
	if !_exists {
		err = os.MkdirAll(b.Path.Data, _folderRight)
		if err != nil {
			return
		}
	}
	// [Path] Log
	_exists, _ = util.IsFileExists(b.Path.Log)
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
func (b *Broker) populateBrokerIDs() (err error) {
	// "go tool dist list" - list all valid OS architecture
	// a. generate id
	err = b.generateIDs()
	if err != nil {
		return
	}
	// replace ENV VARS from the config strings (needs to update manually....)
	// [DOC]: study on using reflection ? (but performance penalty)
	_re, err := regexp.Compile(`\$\{[a-z|A-Z|_|-]+\}`)
	if err != nil {
		return
	}
	b.Name = replaceEnvVarValuesToString(b.Name, _re, b.ID)
	b.Cluster.Name = replaceEnvVarValuesToString(b.Cluster.Name, _re, b.ID)
	b.Network.HostName = replaceEnvVarValuesToString(b.Network.HostName, _re, b.ID)
	b.Network.Port, err = strconv.Atoi(replaceEnvVarValuesToString(strconv.Itoa(b.Network.Port), _re, b.ID))
	if err != nil {
		return
	}
	b.Path.Data = fmt.Sprintf("%v%v%v", replaceEnvVarValuesToString(b.Path.Data, _re, b.ID), string(os.PathSeparator), b.ID)
	b.Path.Log = fmt.Sprintf("%v%v%v", replaceEnvVarValuesToString(b.Path.Log, _re, b.ID), string(os.PathSeparator), b.ID) // replaceEnvVarValuesToString(b.Path.Log, _re, b.ID)

	// TODO: update the setters when new config values are available

	// b. {home}/.quebroker exists? logics inside createFolders()
	err = b.createFolders()
	if err != nil {
		return
	}

	// c. create the "id" files
	_exists, _homePath := util.IsFileExists(b.Path.Data, brokerIDFile)
	log.Tracef("[populateBrokerIDs] *** exists and path? %v - %v\n", _exists, _homePath)
	if !_exists {
		_umaskOld := syscall.Umask(0) // resetting the umask on creating file's permission
		// e. create the id files under the home folder (.quebroker)
		err = b.createIDFile("", _homePath, b.ID)
		if err != nil {
			return
		}
		err = b.createIDFile(b.Path.Data, brokerClusterIDFile, b.Cluster.ID)
		if err != nil {
			return
		}
		// reset the umask
		syscall.Umask(_umaskOld)
	} else {
		// load the .broker.id and .cluster.id file values back to the Broker instance
		b.ID, err = b.readIDFromFile("", _homePath)
		if err != nil {
			return
		}
		b.Cluster.ID, err = b.readIDFromFile(b.Path.Data, brokerClusterIDFile)
		if err != nil {
			return
		}
	}
	return
}

// generateIDs - method to generate broker.id and cluster.id (usually for the 1st time)
func (b *Broker) generateIDs() (err error) {
	var _hostName string

	// a. either use broker-name OR get hostname and concat with home.dir, then trim to first 16 characters
	if b.Name != "" {
		_hostName = b.Name
	} else {
		_hostName, err = os.Hostname()
		if err != nil {
			return
		}
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
	_exists, _tomlPath := util.IsFileExists(_tomlPath, brokerConfigToml)

	if !_exists {
		err = fmt.Errorf(`
		config file [%v] NOT exists! Can also check whether the environment variable [%v] has been setup correctly`,
			brokerConfigToml, paramEnvTomlConfigPath)
		return
	}
	// load file contents and populate into the Broker struct
	instance, err = decodeTomlConfig2BrokerStruct(_tomlPath)
	if err != nil {
		return
	}
	// populate id(s)
	err = instance.populateBrokerIDs()
	if err != nil {
		return
	}

	// TODO: other steps
	instance.setupServices()

	return
}

// decodeTomlConfig2BrokerStruct - load the toml' content into a Broker instance
func decodeTomlConfig2BrokerStruct(filepath string) (instance *Broker, err error) {
	instance = NewBroker()
	//err = configor.New(&configor.Config{Debug: true}).Load(instance, filepath)
	err = configor.Load(instance, filepath)

	return
}

// replaceEnvVarValuesToString - substitute the env variable values to the given string
func replaceEnvVarValuesToString(value string, re *regexp.Regexp, brokerID string) (finalValue string) {
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
			//_replace = fmt.Sprintf("%v%v%v%v%v", _replace, string(os.PathSeparator), brokerHomeDir, string(os.PathSeparator), brokerID)
			// [DOC]: structure should be xxx/data/.state, xxx/data/.broker.id, xxx/data/.cluster.id,
			// xxxx/data/{broker.id} <- store actual data files
			_replace = fmt.Sprintf("%v%v%v", _replace, string(os.PathSeparator), brokerHomeDir)
			finalValue = strings.Replace(finalValue, _param, _replace, 1)

		default:
			_replace := os.Getenv(_envParam)
			finalValue = strings.Replace(finalValue, _param, _replace, 1)
		}
	}
	return
}
