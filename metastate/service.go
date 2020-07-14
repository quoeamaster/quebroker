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

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quoeamaster/quebroker/util"
	"github.com/sirupsen/logrus"
)

// Service - implementation of meta-state services
type Service struct {
	broker      IBroker
	log         *logrus.Logger
	states      map[string]interface{} // TODO: is a map enough??? need more features?
	inMemStates map[string]interface{} // states that are volatile and never persisted

	statePathOnDisk string // the location for R/W on states info

	// lock for updating certain attributes
	mux sync.Mutex
}

// New - create instance of meta-state service
func New(brokerHomeDir string, broker IBroker) (s *Service) {
	s = new(Service)
	s.setupLogger()
	s.broker = broker
	// load or create a brand new "states"
	s.states = make(map[string]interface{})
	err := s._loadMetaStates(brokerHomeDir, s.broker.GetBrokerID())
	if err != nil {
		panic(fmt.Errorf("meta-state service bootstrap failed, reason: %v", err))
	}
	// init in-mem states
	s.inMemStates = make(map[string]interface{})

	return
}

// setupLogger - setup logger instance
func (s *Service) setupLogger() {
	s.log = logrus.New()
	s.log.Out = os.Stderr
	s.log.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})
}

// --------------------------------- //
// *** 	API implementations		*** //
// --------------------------------- //

// _loadMetaStates - method to check whether a previous states info stored or not;
//		if so, load it back from file system
func (s *Service) _loadMetaStates(brokerHomeDir, brokerID string) (err error) {
	// a. check whether the state file is available or not
	_homeDir, err := os.UserHomeDir()
	if err != nil {
		return
	}
	s.statePathOnDisk = fmt.Sprintf("%v%v%v%v%v%v%v", _homeDir, string(os.PathSeparator), brokerHomeDir,
		string(os.PathSeparator), brokerID, string(os.PathSeparator), stateFilename)

	if _exists, _ := util.IsFileExists(s.statePathOnDisk); _exists {
		// b. available - load / unmarshal back to map[string]interface{}
		bContent, err := ioutil.ReadFile(s.statePathOnDisk)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bContent, &s.states)
		if err != nil {
			return err
		}
		// assume states (map) populated
		//s.log.Infof("states loaded: <%v>\n", s.states)
	}
	return
}

// _persist - write states back to disk (file),
//		if "updateStateVersion" is true also update the state-version
func (s *Service) _persist(updateStateVersion bool) (err error) {
	var _prevVer string
	var _prevNum int

	// a. update state version too?
	if updateStateVersion {
		_prevVer, _prevNum, err = s.UpsertStateVersionByPrimaryBroker()
		if err != nil {
			return
		}
	}
	// b. write to file
	defer func() {
		if r := recover(); r != nil {
			// reset values
			if updateStateVersion {
				s.states[KeyStateVersion] = _prevVer
				s.states[KeyStateVersionID] = _prevNum
			}
			err = r.(error)
		}
	}()
	bContent, err := json.Marshal(s.states)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(s.statePathOnDisk, bContent, 0644)
	if err != nil {
		return
	}
	//s.log.Error("**** testing", s.GetStateVersion(), s.GetStateVersionID())
	return
}

// UpsertInMem - update/insert based on the given key-value pair BUT does not persist to disk.
// 	Handy for series based updates and the last item to update would call
//		Upsert(string, interface{}, TRUE) to persist all changes to disk
func (s *Service) UpsertInMem(key string, value interface{}, isVolatileState bool) (original interface{}, err error) {
	return s.Upsert(key, value, false, false, isVolatileState)
}

// Upsert - update/insert based on the given key-value pair
func (s *Service) Upsert(key string, value interface{}, needPersist bool, needStateVersionUpdate bool, isVolatileState bool) (original interface{}, err error) {
	if key != "" && value != nil {
		if isVolatileState {
			// non persistable states (e.g. is_primary_key), elected primary related attributes
			s.inMemStates[key] = value
		} else {
			// persistable states (e.g. state version and ID), NON volatile after restart
			s.states[key] = value
		}
	}
	if needPersist {
		s._persist(needStateVersionUpdate)
	}
	return
}

// UpsertSlice - update/insert based on the given key-value pair
func (s *Service) UpsertSlice(key string, value interface{}, isVolatileState bool) (original interface{}, err error) {
	return
}

// UpsertMap - update/insert based on the given key-value pair
func (s *Service) UpsertMap(key string, value interface{}, isVolatileState bool) (original interface{}, err error) {
	return
}

// GetByKey - return the value under the given key
func (s *Service) GetByKey(key string) (value interface{}, isVolatileState bool) {
	if isVolatileState {
		value = s.inMemStates[key]
	} else {
		value = s.states[key]
	}
	return
}

// UpsertStateVersionByPrimaryBroker - update the state-version by the Primary broker
// PS. this method is usually called INDIRECTLY by other UpsertXXX methods instead.
func (s *Service) UpsertStateVersionByPrimaryBroker() (prevState string, prevNum int, err error) {
	// validation on broker status?? (assume validation done earlier by broker itself)
	// a. create the next state-version value -> timestamp and uuid
	_stateVersion, err := uuid.FromBytes([]byte(fmt.Sprintf("%16v", time.Now().UnixNano())[:16]))
	if err != nil {
		return
	}
	// b. increment the state num
	_stateNum := 1
	_prev := s.GetStateVersionID()
	if _prev != nil {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
				s.log.Error("defer exception, ", err, ":", r)
			}
		}()
		// try to convert to number
		if _prev.(string) != "" {
			prevNum, err = strconv.Atoi(_prev.(string))
			if err != nil {
				return
			}
			_stateNum = prevNum + 1
		}

	} else {
		prevNum = -1 // not present (the first state probably)
	}

	// c. set
	s.states[KeyStateVersion] = _stateVersion.String()
	s.states[KeyStateVersionID] = fmt.Sprintf("%v", _stateNum)

	return
}

// GetStateVersion - return the state's version (string)
func (s *Service) GetStateVersion() string {
	return s.states[KeyStateVersion].(string)
}

// GetStateVersionID - return the state's version id / num
func (s *Service) GetStateVersionID() interface{} {
	return s.states[KeyStateVersionID]
}

// TODO: json diff
// TODO: provide a map with keys for updating (means multiple updates in one api call)

// GetElectedPrimaryBrokerInfo - return the elected broker's ID, name, address if Election already DONE.
// Data within the IN-MEM states
func (s *Service) GetElectedPrimaryBrokerInfo() (ID, name, addr string, available bool) {
	_val := s.inMemStates[KeyPrimaryBrokerID]
	// not available (probably election not DONE or config issue hence no cluster formed yet...)
	if _val != nil {
		ID = _val.(string)
		name = s.inMemStates[KeyPrimaryBrokerName].(string)
		addr = s.inMemStates[KeyPrimaryBrokerAddr].(string)
		available = true
	}
	return
}

// GetAvailableBrokersMap - return the map all available broker(s)
func (s *Service) GetAvailableBrokersMap() (brokers map[string]BrokerMeta) {
	_val := s.inMemStates[KeyAvailableBrokers]
	if _val == nil {
		// 1st time access or really no broker(s) available till now
		brokers = make(map[string]BrokerMeta)
		s.inMemStates[KeyAvailableBrokers] = brokers

	} else {
		brokers = _val.(map[string]BrokerMeta)
	}
	return
}

/* -------------------------------------------------------------- */
/*		interface with getters / setters										*/
/*	[DOC] in order to avoid "cycle import"; use interface instead	*/
/* -------------------------------------------------------------- */

// IBroker - broker interface (avoiding import cycles)
type IBroker interface {
	GetConfig(key string) interface{}

	GetBrokerID() string

	//IsElectedPrimaryBroker() bool

	GetBootstrapInitialPrimaryBrokersList() []string

	GetBrokerAddr() string

	GetBrokerName() string

	String() string
}

// BrokerMeta - structure holding a valid Broker's meta information such as NAME, ID, addr, isPrimaryEligible etc
type BrokerMeta struct {
	name              string
	id                string
	addr              string
	isPrimaryEligible bool
}

// GetName - return the broker name
func (m *BrokerMeta) GetName() string {
	return m.name
}

// GetID - return the broker ID
func (m *BrokerMeta) GetID() string {
	return m.id
}

// GetAddr - return the broker addr
func (m *BrokerMeta) GetAddr() string {
	return m.addr
}

// IsPrimaryEligible - return the broker 's election eligibility
func (m *BrokerMeta) IsPrimaryEligible() bool {
	return m.isPrimaryEligible
}

// IsBrokerMetaStructNil - checks whether the given BrokerMeta is nil / empty
func IsBrokerMetaStructNil(val BrokerMeta) (isNil bool) {
	if val.name == "" && val.id == "" && val.addr == "" && val.isPrimaryEligible == false {
		isNil = true
	} else {
		isNil = false
	}
	return
}
