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
	context "context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
)

/* -------------------------------- */
/*		election APIs (server impl)	*/
/* -------------------------------- */

// InitiateElectionRequest - ping other eligible broker(s) for info to start election
func (s *Service) InitiateElectionRequest(ctx context.Context, req *ElectionRequest) (res *Dummy, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	// compare the IDs and the min one wins and became the new Primary broker
	_win := false
	_compResult := strings.Compare(s.broker.GetBrokerID(), req.GetBrokerID())

	if _compResult < 0 {
		_win = true
	} else if _compResult == 0 {
		// use the addr to compare one more time (final bout)
		if strings.Compare(s.broker.GetBrokerAddr(), req.GetBrokerAddr()) < 0 {
			// - win
			_win = true
		}
	}

	// handling election results
	if _win {
		// - win
		// update this broker's primary states
		s.UpsertInMem(KeyPrimaryBroker, true, true)
		s.UpsertInMem(KeyPrimaryBrokerID, s.broker.GetBrokerID(), true)
		s.UpsertInMem(KeyPrimaryBrokerName, s.broker.GetBrokerName(), true)
		s.Upsert(KeyPrimaryBrokerAddr, s.broker.GetBrokerAddr(), true, true, true)

	} else {
		// - lose
		// issue ACK request instead
		_gConn, err2 := grpc.Dial(req.GetBrokerAddr(), grpc.WithInsecure())
		defer _gConn.Close()

		if err2 != nil {
			s.log.Errorf("[InitiateElectionRequest] unusual situation since the requester broker is offlined?? %v\n", err2)
			err = err2
			return
		}
		_srv := NewMetastateServiceClient(_gConn)
		_resp, err3 := _srv.GetElectedPrimaryACK(context.Background(), &ElectionDoneHandshakeRequest{
			PrimaryBrokerID:   req.GetBrokerID(),
			PrimaryBrokerName: req.GetBrokerName(),
			PrimaryBrokerAddr: req.GetBrokerAddr(),
			SrcBrokerID:       s.broker.GetBrokerID(),
			SrcBrokerName:     s.broker.GetBrokerName(),
			SrcBrokerAddr:     s.broker.GetBrokerAddr(),
		})
		if err3 != nil {
			s.log.Errorf("[InitiateElectionRequest] ACK request exception, reason: %v\n", err3)
			err = err3
			return
		}
		// validate the response and update the state's elected primary attributes
		if _resp.Code == ackStatusCode200 {
			// update this broker's primary states
			s.UpsertInMem(KeyPrimaryBroker, false, true)
			s.UpsertInMem(KeyPrimaryBrokerID, req.GetBrokerID(), true)
			s.UpsertInMem(KeyPrimaryBrokerName, req.GetBrokerName(), true)
			s.UpsertInMem(KeyPrimaryBrokerAddr, req.GetBrokerAddr(), true)
			// version number and ID (running num)
			s.UpsertInMem(KeyStateVersion, _resp.GetStateVersion(), false)
			s.Upsert(KeyStateVersionID, _resp.GetStateNum(), true, false, false)
		} else {
			// other status code should be EXCEPTION
			err = fmt.Errorf("[InitiateElectionRequest] ACK request exception, code [%v] - [%v]",
				_resp.Code, s._electionDoneACKRespStatusTranslator(_resp.Code))
			s.log.Error(err)
			return
		}
	}
	s.log.Infof("[InitiateElectionRequest] finally ... state [%v] vs in-mem state [%v]\n", s.states, s.inMemStates)
	return
}

// GetElectedPrimaryACK - for non winners, ping back the elected primary for ACK
func (s *Service) GetElectedPrimaryACK(context context.Context, req *ElectionDoneHandshakeRequest) (res *ElectionDoneHandshakeACKResponse, err error) {
	// a. add back this brokers info into the inMem States
	_brokersMap := s.GetAvailableBrokersMap()
	_brokerKey := req.SrcBrokerID
	if IsBrokerMetaStructNil(_brokersMap[_brokerKey]) {
		// not available in the map yet, add it ~
		_brokersMap[_brokerKey] = BrokerMeta{
			name:              req.SrcBrokerName,
			id:                req.SrcBrokerID,
			addr:              req.SrcBrokerAddr,
			isPrimaryEligible: true,
		}
	}

	// b. return ACK Response
	res = &ElectionDoneHandshakeACKResponse{
		Code:         int32(ackStatusCode200),
		StateVersion: s.GetStateVersion(),
		StateNum:     s.GetStateVersionID().(int32),
	}
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
		s.mux.Lock()
		if _, _, _, _found := s.GetElectedPrimaryBrokerInfo(); _found {
			s.mux.Unlock()
			break
		}
		s.mux.Unlock()

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

			// the Timer random interval (kind of throttle before issuing the dial)
			<-s._getRandomTimer().C

			_, err := _srv.InitiateElectionRequest(context.Background(), &ElectionRequest{
				BrokerName: s.broker.GetBrokerName(),
				BrokerID:   s.broker.GetBrokerID(),
				BrokerAddr: _localAddr})
			if err != nil {
				// mostly unreachable scenarios (but not necessary to be an error, maybe the target broker not yet started up...)
				s.log.Warnf("[ClientInitElectionRequest] init election failed, reason: %v\n", err)
				continue
			}
		}
		_retry++ // update the retry counter
	}

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

// _updateToElectedMasterState - method to update the "is_primary_broker" state
func (s *Service) _updateToElectedMasterState() {
	_, err := s.Upsert(KeyPrimaryBroker, true, true, true, true)
	if err != nil {
		panic(fmt.Errorf("[clientInitElectionRequest] set meta state exception, reason: %v", err))
	}
	// TODO: set back to DEBUG level
	s.log.Infof("[_updateToElectedMasterState] TBD elected primary discovered, meta state as is: [%v]; in-mem states: [%v]", s.states, s.inMemStates)
}

// create a random timer to throttle the ping / dial / connection
func (s *Service) _getRandomTimer() (timer *time.Timer) {
	_seed := rand.NewSource(time.Now().UnixNano())
	_r := rand.New(_seed)
	// at least 1.5 sec, max within 2.0 sec
	// (interval/4 -> 500ms; interval/4*3 -> 1500ms = sum is roughly within 2000ms)
	_randomInterval := _r.Intn(intervalElectionDial/4) + intervalElectionDial/4*3
	s.log.Tracef("[_getRandomTimer] timer interval: [%v]\n", _randomInterval)
	timer = time.NewTimer(time.Duration(_randomInterval) * time.Millisecond)

	return
}

/* ----------------------------------------------- */
/*		cluster forming / joining APIs (server impl)	*/
/* ----------------------------------------------- */

// InitiateClusterJoin - for non eligibe broker(s); initiate this request to join the Cluster
// MUST check the status returned to decide whether to resend join request again
// TODO: implementation
func (s *Service) InitiateClusterJoin(context context.Context, req *ClusterJoinRequest) (res *ClusterJoinResponse, err error) {
	if id, name, addr, avail := s.GetElectedPrimaryBrokerInfo(); avail {
		res = &ClusterJoinResponse{
			PrimaryBrokerID:   id,
			PrimaryBrokerName: name,
			PrimaryBrokerAddr: addr,
			Status:            joinStatusCode200,
		}
		// brokersMap serialization
		_brokerMetaList := make([]string, 0)
		_brokersMap := s.GetAvailableBrokersMap()
		for _, _bMeta := range _brokersMap {
			_brokerMetaList = append(_brokerMetaList, _bMeta.SerializeToString())
		}
		res.BrokersMap = _brokerMetaList

	} else if !avail {
		// in progress
		res = &ClusterJoinResponse{
			Status: joinStatusCode300,
		}
	}
	return
}

// ClientInitClusterJoinRequest - initiate the join cluster request by non primary-eligible broker(s)
func (s *Service) ClientInitClusterJoinRequest() {
	_tBrokers := s.broker.GetBootstrapInitialPrimaryBrokersList()
	_gConns := make([]*grpc.ClientConn, len(_tBrokers))
	defer func() {
		for _, _gConn := range _gConns {
			if _gConn != nil {
				_gConn.Close()
			}
		}
	}()
	_srvs := make([]MetastateServiceClient, len(_tBrokers))

	for true {
		// elected primary info ready - exit the endless join loop
		s.mux.Lock()
		if _id, _, _, _avail := s.GetElectedPrimaryBrokerInfo(); _avail {
			s.mux.Unlock()

			s.log.Infof("[ClientInitClusterJoinRequest] %v(%v) joined the cluster. Primary broker ID: %v\n",
				s.broker.GetBrokerName(), s.broker.GetBrokerAddr(), _id)
			break
		}
		s.mux.Unlock()

		// a. dial / connect to each targeted broker
		for i, _tBroker := range _tBrokers {
			_srv := _srvs[i]
			if _srv == nil {
				_gConn, err := grpc.Dial(_tBroker, grpc.WithInsecure())
				if err != nil {
					s.log.Warnf("[ClientInitClusterJoinRequest] connection error, reason: %v\n", err)
					continue
				}
				_srv := NewMetastateServiceClient(_gConn)
				_gConns[i] = _gConn
				_srvs[i] = _srv
			}
			// send join request
			<-s._getRandomTimerForJoinCluster().C

			_resp, err := _srv.InitiateClusterJoin(context.Background(), &ClusterJoinRequest{
				BrokerID:   s.broker.GetBrokerID(),
				BrokerName: s.broker.GetBrokerName(),
				BrokerAddr: s.broker.GetBrokerAddr(),
			})
			if err != nil {
				// mostly connectivity issues, though very rare (should already failed earlier)
				s.log.Errorf("[ClientInitClusterJoinRequest] rare case, could not connect to the target broker[%v], reason:[%v]\n", _tBroker, err)
				continue
			}
			// status check
			if _resp.Status == joinStatusCode200 {
				// all done ; deserialization of all available brokers []string back to map in the in-mem state
				//_brokersMap := s.GetAvailableBrokersMap()
				_brokersMap := make(map[string]BrokerMeta)
				for _, _bData := range _resp.BrokersMap {
					_bMeta := new(BrokerMeta)
					if err2 := _bMeta.DeserializeFromString(_bData); err2 != nil {
						s.log.Warnf("[ClientInitClusterJoinRequest] deserialization of BrokerMeta [%v] failed [%v]\n", _bData, err2)
					} else {
						_brokersMap[_bMeta.GetID()] = *_bMeta
					}
				}
				s.mux.Lock()
				s.inMemStates[KeyAvailableBrokers] = _brokersMap
				s.mux.Unlock()

			} else if _resp.Status == joinStatusCode300 {
				// in progress (continue to ping again till election done etc)
				continue
			} else {
				// error
				s.log.Errorf("[ClientInitClusterJoinRequest] %v - %v", _resp.Status, s._joinRespStatusTranslator(_resp.Status))
				return // TODO: panic instead???
			} // end -- if (resp status)
		}
	}
	// anything else???
}

// create a random timer to throttle the ping / dial / connection
func (s *Service) _getRandomTimerForJoinCluster() (timer *time.Timer) {
	_seed := rand.NewSource(time.Now().UnixNano())
	_r := rand.New(_seed)
	// at least 3 sec, max within 4 sec
	// (intervalJoinClusterDial/4 -> 1000ms; intervalJoinClusterDial/5*3 -> 3000ms = sum is roughly within 4000ms)
	_randomInterval := _r.Intn(intervalJoinClusterDial/5) + intervalJoinClusterDial/5*3
	s.log.Infof("[_getRandomTimerForJoinCluster] TBD timer interval: [%v]\n", _randomInterval)
	timer = time.NewTimer(time.Duration(_randomInterval) * time.Millisecond)

	return
}

/* -------------------------------------- */
/*		response status-code translator		*/
/* -------------------------------------- */

// _electionDoneACKRespStatusTranslator - return the translated message based on status-code for election ACK request
func (s *Service) _electionDoneACKRespStatusTranslator(code int32) (msg string) {
	switch code {
	case ackStatusCode200:
		msg = "OK - success"
	case ackStatusCode500:
		msg = "General Server side error"
		// TODO: update this when more status(s) are formalized
	default:
		msg = "UNKNOWN status"
	}
	return
}

// _joinRespStatusTranslator - return the translated message based on status-code for Joining cluster request
func (s *Service) _joinRespStatusTranslator(code int32) (msg string) {
	switch code {
	case joinStatusCode200:
		msg = "OK - success"
	case joinStatusCode300:
		msg = "In progress, Primary Broker not ELECTED yet"
	case joinStatusCode500:
		msg = "General Server side error"
		// TODO: update this when more status(s) are formalized
	default:
		msg = "UNKNOWN status"
	}
	return
}
