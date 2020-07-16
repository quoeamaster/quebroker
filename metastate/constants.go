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

package metastate

// during Election ping / dial; the max no. of retries is 50 time, roughly 1.5 minutes
// (assume each connection ping / dial is based on a random interval within 2 seconds)
// TODO: should be 50 for production -> const maxElectionDialRetrial = 50
// 30
const maxElectionDialRetrial = 5

// if broadcast requests failed, max retry 3 times ONLY and then mark offline the problematic broker
const maxBroadcastRetrial = 3

// each connection ping / dial is based on a random interval within 2 seconds (2000 ms)
const intervalElectionDial = 2000

// each broadcast retry within 1500ms
const intervalBroadcastDial = 1500

// each connection ping / dial is based on a random interval within 5 seconds (2000 ms)
const intervalJoinClusterDial = 5000

// ack response status 200 = OK
const ackStatusCode200 = 200

// ack response status 500 = generic server side error (no reason stated)
const ackStatusCode500 = 500

// join status 200 -> ok, all done
const joinStatusCode200 = 200

// join status code 300 -> in progress; primary broker not elected yet
const joinStatusCode300 = 300

// join status code 500 -> general server side error (no specific reason stated)
const joinStatusCode500 = 500

// forward request / response code 200 -> OK
const fwdStatusCode200 = 200

// forward request / response code 500 -> general server error
const fwdStatusCode500 = 500

// broadcast status code 200 - OK
const broadcastStatusCode200 = 200

// broadcast status code 500 - general server error
const broadcastStatusCode500 = 500

const stateFilename = ".state" // the persisted state file's name
// KeyStateVersion - the state(s) current version; for checking which broker's state is the most updated or
//	simply would a diff be required (if 2 broker's state_version are different, should be the same - in sync)
const KeyStateVersion = "state_version"

// KeyStateVersionID - the running number of the state version
const KeyStateVersionID = "state_version_id"

// KeyPrimaryBroker - stating if this broker instance is the PRIMARY (like commander of a team)
// Primary broker is supposed to handle:
// - initial writes (add, update, delete of queue messages)
// - approve changes (like change in partition numbers of the queue / topic etc)
// - decision making / coordination
const KeyPrimaryBroker = "is_primary_broker"

// KeyPrimaryBrokerName - the elected primary's name
const KeyPrimaryBrokerName = "primary_broker_name"

// KeyPrimaryBrokerID - the elected primary's ID
const KeyPrimaryBrokerID = "primary_broker_id"

// KeyPrimaryBrokerAddr - the elected primary's address (ip:port format)
const KeyPrimaryBrokerAddr = "primary_broker_addr"

// KeyAvailableBrokers - the available brokers till now
const KeyAvailableBrokers = "available_brokers"
