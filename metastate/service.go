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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/quoeamaster/quebroker/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Service - implementation of meta-state services
type Service struct {
	broker IBroker
	log    *logrus.Logger
	states map[string]interface{} // TODO: is a map enough??? need more features?

	statePathOnDisk string // the location for R/W on states info
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
func (s *Service) UpsertInMem(key string, value interface{}) (original interface{}, err error) {
	return s.Upsert(key, value, false, false)
}

// Upsert - update/insert based on the given key-value pair
func (s *Service) Upsert(key string, value interface{}, needPersist bool, needStateVersionUpdate bool) (original interface{}, err error) {
	if key != "" && value != nil {
		s.states[key] = value
	}
	if needPersist {
		s._persist(needStateVersionUpdate)
	}
	return
}

// UpsertSlice - update/insert based on the given key-value pair
func (s *Service) UpsertSlice(key string, value interface{}) (original interface{}, err error) {
	return
}

// UpsertMap - update/insert based on the given key-value pair
func (s *Service) UpsertMap(key string, value interface{}) (original interface{}, err error) {
	return
}

// GetByKey - return the value under the given key
func (s *Service) GetByKey(key string) (value interface{}) {
	value = s.states[key]
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

/* -------------------------------- */
/*		election APIs (server impl)	*/
/* -------------------------------- */

// InitiateElectionRequest - ping other eligible broker(s) for info to start election
func (s *Service) InitiateElectionRequest(context context.Context, req *ElectionRequest) (res *Dummy, err error) {
	return
}

// GetElectedPrimaryACK - for non winners, ping back the elected primary for ACK
func (s *Service) GetElectedPrimaryACK(context context.Context, req *ElectionDoneHandshakeRequest) (res *ElectionDoneHandshakeACKResponse, err error) {
	return
}

/* -------------------------------- */
/*		election APIs (client impl)	*/
/* -------------------------------- */

// ClientInitElectionRequest - init the election request ...
func (s *Service) ClientInitElectionRequest() {
	_brokers := s.broker.GetBootstrapInitialPrimaryBrokersList()
	_localAddr := s.broker.GetBrokerAddr()

	// extreme case, only 1 entry and it yields the same address; this instance is the ELECTED primary!
	if len(_brokers) == 1 && strings.Compare(_localAddr, _brokers[0]) == 0 {
		s._updateToElectedMasterState()
		return
	}

	_targetBrokers := make([]string, 0)
	// remove the local addr one (no point to ping itself... right???)
	for _, _addr := range _brokers {
		if strings.Compare(_addr, _localAddr) != 0 {
			_targetBrokers = append(_targetBrokers, _addr)
		}
	}
	// another extreme case... the original contents of the bootstrap list WAS also the same as _localAddr
	if len(_targetBrokers) == 0 {
		s._updateToElectedMasterState()
		return
	}

	// start the request polling
	_srvs := make([]MetastateServiceClient, len(_targetBrokers))
	_gConns := make([]*grpc.ClientConn, len(_targetBrokers))
	_retry := 0
	for true {
		// primary elected + ACK received
		if _, _, _, _found := s.GetElectedPrimaryBrokerInfo(); _found {
			break
		}
		// extreme case, no other broker's available for ping / dial / connection
		// so the local broker is the Elected Primary
		if _retry >= maxElectionDialRetrial {
			s._updateToElectedMasterState()
			break
		}

		// _tBroker = the target broker's address
		for i, _tBroker := range _targetBrokers {
			// create connection and store it for re-use
			_srv := _srvs[i]
			if _srv == nil {
				_gConn, err := grpc.Dial(_tBroker, grpc.WithInsecure())
				if err != nil {
					s.log.Warnf("[ClientInitElectionRequest] could not connect with [%v]\n", _tBroker)
					continue
				}
				_srv = NewMetastateServiceClient(_gConn)
				_srvs[i] = _srv
				_gConns[i] = _gConn // required... as you need to clean up the ClientConn after election done
			} // if (create grpc.ClientConn and set _srvs content)

			// TODO: implement the server-side InitiElectionRequest too!
			// TODO: the Timer random interval ~~~~~~~ DO it first~
			_, err := _srv.InitiateElectionRequest(context.Background(), &ElectionRequest{
				BrokerName: s.broker.GetBrokerName(),
				BrokerID:   s.broker.GetBrokerID(),
				BrokerAddr: _localAddr})
			if err != nil {
				// mostly unreachable scenarios (but not necessary to be an error, maybe the target broker not yet started up...)
				s.log.Warnf("[ClientInitElectionRequest] initiate election request failed, reason: %v\n", err)
				continue
			}
		}
		// update the retry counter
		_retry++
	} // while true... loop with breaks

	// cleanup
	for _, _gConn := range _gConns {
		if err2 := _gConn.Close(); err2 != nil {
			s.log.Warnf("[ClientInitElectionRequest] try to close the corresponding grpc connection, but got exception with reason: [%v]\n", err2)
		}
	}
	_gConns = nil
	_srvs = nil

	// ... anything else???

}
func (s *Service) _updateToElectedMasterState() {
	_, err := s.Upsert(KeyPrimaryBroker, true, true, true)
	if err != nil {
		panic(fmt.Errorf("[clientInitElectionRequest] set meta state exception, reason: %v", err))
	}
	s.log.Infof("[_updateToElectedMasterState] TBD elected primary discovered, meta state as is: [%v]", s.states)
}

/* ----------------------------------------------- */
/*		cluster forming / joining APIs (server impl)	*/
/* ----------------------------------------------- */

// InitiateClusterJoin - for non eligibe broker(s); initiate this request to join the Cluster
// MUST check the status returned to decide whether to resend join request again
func (s *Service) InitiateClusterJoin(context context.Context, req *ClusterJoinRequest) (res *ClusterJoinResponse, err error) {
	return
}

func (s *Service) ClientInitClusterJoinRequest() {}

// GetElectedPrimaryBrokerInfo - return the elected broker's ID, name, address if Election already DONE.
func (s *Service) GetElectedPrimaryBrokerInfo() (ID, name, addr string, available bool) {
	_val := s.states[KeyPrimaryBrokerID]
	// not available (probably election not DONE or config issue hence no cluster formed yet...)
	if _val != nil {
		ID = _val.(string)
		name = s.states[KeyPrimaryBrokerName].(string)
		addr = s.states[KeyPrimaryBrokerAddr].(string)
		available = true
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

	IsElectedPrimaryBroker() bool

	GetBootstrapInitialPrimaryBrokersList() []string

	GetBrokerAddr() string

	GetBrokerName() string
}
