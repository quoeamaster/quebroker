/*
 *  Copyright Project - queBroker, Author - quoeamaster, (C) 2018
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
    "github.com/quoeamaster/CFactor/TOML"
    "queutil"
    "strings"
    "runtime"
    "fmt"
    "reflect"
)

// environment variable pointing to the path of the que broker config file
const envVarBrokerConfigPath = "QUE_BROKER_CONFIG_PATH"

// default broker config file (unix based os)
const defaultBrokerConfigPathUnix = "/usr/local/que/queBroker.toml"
// default broker config file (windows based os)
const defaultBrokerConfigPathWindows = "c:\\que\\queBroker.toml"

// locat broker config file (the relative path)
const localBrokerConfigPath = "queBroker.toml"




type BrokerConfig struct {
    // path of the config file
    path string

    // name of the broker for identification
    // (name could be duplicated, in general Que will identify a broker by UUID,
    // name is for readability purpose)
    BrokerName string `toml:"broker.name"`
}

// a must provided lifecyclehook method
func (o *BrokerConfig) SetStructsReferences(refs *map[string]interface{}) error {
    return nil
}


// create a new instane of the broker config wrapper
func NewBrokerConfig(path string) (*BrokerConfig, error) {
    m := new(BrokerConfig)

    configPath, err := m.loadConfig(path)
    if err != nil {
        return nil, err
    }
    m.path = configPath

    return m, nil
}

func (b *BrokerConfig) loadConfig(path string) (string, error) {
    finalPath := ""

    if queutil.IsFileExists(path) == true {
        // a) is given path valid?
        finalPath = path
    } else if queutil.IsFileExists(localBrokerConfigPath) == true {
        // b) local broker config
        finalPath = localBrokerConfigPath
    } else {
        // c) the default config location based on os
        if strings.Compare(runtime.GOOS, "windows") == 0 {
            if queutil.IsFileExists(defaultBrokerConfigPathWindows) == true {
                finalPath = defaultBrokerConfigPathWindows
            }
        } else {
            if queutil.IsFileExists(defaultBrokerConfigPathUnix) == true {
                finalPath = defaultBrokerConfigPathUnix
            }
        }
    }
    // is finalPath valid?
    if strings.Compare(finalPath, "") == 0 {
        return "", fmt.Errorf("could not find a valid config file for the broker")
    }
    // try to read the config at relative local path
    configInstance := *b
    configReader := TOML.NewTOMLConfigImpl(finalPath, reflect.TypeOf(configInstance))

    _, err := configReader.Load(b)
    if err != nil {
        return "", err
    }

    return finalPath, nil
}

// return the config file's path (mainly for information purpose)
func (b *BrokerConfig) GetPath() string {
    return b.path
}
