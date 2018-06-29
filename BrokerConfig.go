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

const envVarBrokerConfigPath = "QUE_BROKER_CONFIG_PATH"
const defaultBrokerConfigPath = "/usr/local/que/queBroker.toml"

type BrokerConfig struct {
    // path of the config file
    path string

}

// the actual struct wrapping the config file (toml)
type BrokerConfigContent struct {
    // name of the broker for identification
    // (name could be duplicated, in general Que will identify a broker by UUID,
    // name is for readability purpose)
    BrokerName string `toml:"broker.name"`

}

func NewBrokerConfig() *BrokerConfig {
    m := new(BrokerConfig)



    return m
}

// return the config file's path (mainly for information purpose)
func (b *BrokerConfig) GetPath() string {
    return b.path
}
