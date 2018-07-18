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
    "bytes"
)

// environment variable pointing to the path of the que broker config file
const envVarBrokerConfigPath = "QUE_BROKER_CONFIG_PATH"

// the path containing the config files under the current user's home directory
const homeDirectoryConfigDir = ".que"

// local broker config file (the relative path)
const localBrokerConfigPath = "queBroker.toml"




type BrokerConfig struct {
    // path of the config file
    path string

    // name of the broker for identification
    // (name could be duplicated, in general Que will identify a broker by UUID,
    // name is for readability purpose)
    BrokerName string `toml:"broker.name"`

    // the path for storing all sorts of logs
    LogFolder string `toml:"log.path"`
    // the number of rotated files kept (default is 3)
    LogRotationDays int `toml:"log.rotation.days"`
    // the max file size per log
    LogMaxFileSizeInMb int `toml:"log.max.size.mb"`
    // the default log level (logs with level equals to or greater than this value would be logged)
    LogLevel string `toml:"log.level"`

    // the path for storing the messages
    DataFolder string `toml:"data.path"`

    // the broker's accessible address (communicate with clients or
    // other Brokers within the cluster), format is host|ip:portNo
    BrokerCommunicationAddress string `toml:"broker.communication.address"`

    // cluster name
    ClusterName string `toml:"cluster.name"`
    // "simple" cluster discovery's seed list of broker address(s)
    ClusterDiscoverySimpleSeeds []string `toml:"cluster.discovery.simple.seeds"`
}

// a must provided lifecyclehook method
func (o *BrokerConfig) SetStructsReferences(refs *map[string]interface{}) error {
    return nil
}

func (o *BrokerConfig) String() string {
    var buf bytes.Buffer

    buf.WriteString("\n")
    buf.WriteString("------------------------------\n")
    buf.WriteString("|     configuration  set     |\n")
    buf.WriteString("------------------------------\n")

    buf.WriteString(fmt.Sprintf("%v => %v\n\n", "* CONFIG FILE PATH", o.path))

    //buf.WriteString(fmt.Sprintf("%v %80s", "broker.name", "= " + o.BrokerName))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "cluster.name", o.ClusterName))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "cluster.discovery.simple.seeds", o.ClusterDiscoverySimpleSeeds))

    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "broker.name", o.BrokerName))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "broker.communication.address", o.BrokerCommunicationAddress))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "log.path", o.LogFolder))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "log.rotation.days", o.LogRotationDays))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "log.max.size.mb", o.LogMaxFileSizeInMb))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "log.level", o.LogLevel))
    buf.WriteString(fmt.Sprintf("%-30v = %v\n", "data.path", o.DataFolder))


    buf.WriteString("\n")
    return buf.String()
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

// load the config file based on the given path; the sequences are as follows:

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
        userHomeDir, err := queutil.GetCurrentUserHomeDir()
        if err != nil {
            return "", err
        }
        if strings.Compare(runtime.GOOS, "windows") == 0 {
            hdFile := userHomeDir + "\\" + homeDirectoryConfigDir + "\\" + localBrokerConfigPath
            if queutil.IsFileExists(hdFile) == true {
                finalPath = hdFile
            }
        } else {
            hdFile := userHomeDir + "/" + homeDirectoryConfigDir + "/" + localBrokerConfigPath
            if queutil.IsFileExists(hdFile) == true {
                finalPath = hdFile
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
