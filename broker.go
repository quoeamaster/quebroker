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
	"strings"
)

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
	// TODO: update String method when new members are added
	var _b strings.Builder

	_b.WriteString("broker instance:\n")
	_b.WriteString(fmt.Sprintf("  id [%v], name [%v]\n", b.ID, b.Name))
	_b.WriteString(fmt.Sprintf("  cluster.id [%v], cluster.name [%v]\n", b.Cluster.ID, b.Cluster.Name))
	_b.WriteString(fmt.Sprintf("  boostrap.server.list [%v] of size %v\n", b.BootstrapServers, len(b.BootstrapServers)))
	_b.WriteString(fmt.Sprintf("  network.hostname [%v], network.port [%v]\n", b.Network.HostName, b.Network.Port))
	_b.WriteString(fmt.Sprintf("  path.data [%v], path.log [%v]", b.Path.Data, b.Path.Log))

	_b.WriteString("\n")
	return _b.String()
}
