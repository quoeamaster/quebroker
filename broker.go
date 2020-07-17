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

	"github.com/quoeamaster/quebroker/metastate"
	"github.com/quoeamaster/quebroker/vision"
)

var log = GetBasicTextLogger()

// Broker - representing a broker instance
type Broker struct {
	// actual UUID of the broker (however broker_id is SET automatically ONLY for the 1st time)
	ID string `default:""`
	// name of broker for human readability
	Name string `required:"true"`

	//BootstrapServers []string

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

	Bootstrap struct {
		// the servers to handshake when joining or creating the cluster
		// also they act as the candidates for being elected as PRIMARY as well (one of them would be the PRIMARY)
		InitialPrimaryBrokersList []string `required:"true"`
	}

	// is this instance a potential PRIMARY (either elected or backup / potential)
	isPrimaryCandidate int
	// is this instance the ELECTED PRIMARY (elected or backup / potential)
	isPrimaryElected bool
	// the broker's actual address
	addr string

	// service(s)
	Vision    *vision.Service    // service for vision API
	MetaState *metastate.Service // service for metaState handling
	// TODO: update the service(s) available
}

// String - description of a Broker
func (b *Broker) String() string {
	// TODO: update String method when new members are added
	var _b strings.Builder

	_b.WriteString("broker instance:\n")
	_b.WriteString(fmt.Sprintf("  id [%v], name [%v]\n", b.ID, b.Name))
	_b.WriteString(fmt.Sprintf("  is eligible for Primary Broker election? [%v], broker address [%v]\n", b.GetIsPrimaryCandidate(), b.GetBrokerAddr()))
	_b.WriteString(fmt.Sprintf("  cluster.id [%v], cluster.name [%v]\n", b.Cluster.ID, b.Cluster.Name))
	_b.WriteString(fmt.Sprintf("  network.hostname [%v], network.port [%v]\n", b.Network.HostName, b.Network.Port))
	_b.WriteString(fmt.Sprintf("  path.data [%v], path.log [%v]\n", b.Path.Data, b.Path.Log))
	_b.WriteString(fmt.Sprintf("  bootstrap.initialPrimaryBrokersList [%v] of size [%v]\n",
		b.Bootstrap.InitialPrimaryBrokersList, len(b.Bootstrap.InitialPrimaryBrokersList)))

	return _b.String()
}

// NewBroker - constructor, initializing attributes with defaults when necessary
func NewBroker() (instance *Broker) {
	instance = new(Broker)
	instance.isPrimaryCandidate = -1  // -1 means not yet check (the check should only happen once)
	instance.isPrimaryElected = false // default is FALSE (set after a real election happened)
	return
}

// GetIsPrimaryCandidate - check whether this broker instance could be a potential PRIMARY candidate
func (b *Broker) GetIsPrimaryCandidate() (isPrimary bool) {
	// not yet check (1st time check)
	if b.isPrimaryCandidate == -1 {
		//_addr := fmt.Sprintf("%v:%v", b.Network.HostName, b.Network.Port)
		_addr := b.GetBrokerAddr()
		for _, iPrimary := range b.Bootstrap.InitialPrimaryBrokersList {
			if strings.Compare(iPrimary, _addr) == 0 {
				b.isPrimaryCandidate = 1
				break
			}
		}
		// not a single match found within the initial possible PRIMARY broker(s)
		if b.isPrimaryCandidate == -1 {
			b.isPrimaryCandidate = 0
		}
	}
	// final decision
	if b.isPrimaryCandidate == 0 {
		isPrimary = false
	} else {
		isPrimary = true
	}
	return
}

// Stop - stop or exit the broker sequence
func (b *Broker) Stop() {
	// TODO: TBD
	log.Info("heya, inside broker.stop")
}

func (b *Broker) setupServices() {
	// TODO: update service definitions here
	log.Trace("setup on services")
	b.Vision = vision.New()
	b.MetaState = metastate.New(b.Path.Data, b)
}

/* ----------------------------------------------------------- */
/*		interface with getters / setters	(avoid cycle imports)	*/
/* ----------------------------------------------------------- */

// GetConfig - return the value based on the key, could be nil
func (b *Broker) GetConfig(key string) (value interface{}) {
	log.Error("[GetConfig] not yet implemented...")
	return
}

// GetBrokerID - return the broker-id
func (b *Broker) GetBrokerID() (value string) {
	value = b.ID
	return
}

// IsElectedPrimaryBroker - whether this broker instance is ELECTED primary broker
// threadSafe
/*
func (b *Broker) IsElectedPrimaryBroker() bool {
	b.mux.Lock()
	defer b.mux.Unlock()

	_val := b.MetaState.GetByKey(metastate.KeyPrimaryBroker)
	if _val != nil {
		return _val.(bool)
	} else {
		// set the value to false... (usually only 1st time access would yield a nil)
		b.MetaState.Upsert(metastate.KeyPrimaryBroker, false, true, false)
		return false
	}
}
*/

// GetBootstrapInitialPrimaryBrokersList - return the bootstrap broker list
func (b *Broker) GetBootstrapInitialPrimaryBrokersList() []string {
	return b.Bootstrap.InitialPrimaryBrokersList
}

// GetBrokerAddr - return the broker's address
func (b *Broker) GetBrokerAddr() string {
	if b.addr == "" {
		b.addr = fmt.Sprintf("%v:%v", b.Network.HostName, b.Network.Port)
	}
	return b.addr
}

// GetBrokerName - return the broker's name
func (b *Broker) GetBrokerName() string {
	return b.Name
}
