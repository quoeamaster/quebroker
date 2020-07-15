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
	"strconv"
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
		s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())

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
			s._updateElectionWonInMemStates(req.GetBrokerID(), req.GetBrokerName(), req.GetBrokerAddr())

			// version number and ID (running num)
			s.UpsertInMem(KeyStateVersion, _resp.GetStateVersion(), false)
			s.Upsert(KeyStateVersionID, fmt.Sprintf("%v", _resp.GetStateNum()), true, false, false)
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
		s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())
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
		s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())
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
			s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())
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
				s.log.Debugf("[ClientInitElectionRequest] init election failed, reason: %v\n", err)
				s.log.Infof("[ClientInitElectionRequest] dialing to target broker [%v] failed... retry initiated\n", _tBroker)
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

/* ----------------------------------------------- */
/*		cluster forming / joining APIs (server impl)	*/
/* ----------------------------------------------- */

// InitiateClusterJoin - for non eligibe broker(s); initiate this request to join the Cluster
// MUST check the status returned to decide whether to resend join request again
func (s *Service) InitiateClusterJoin(ctx context.Context, req *ClusterJoinRequest) (res *ClusterJoinResponse, err error) {
	s.log.Debugf("[InitiateClusterJoin] in-mem state [%v]\n", s.inMemStates)
	//_id, _, _, _avail := s.GetElectedPrimaryBrokerInfo()
	//s.log.Warn("*** vals :", _id, " ", _avail)

	if id, name, addr, avail := s.GetElectedPrimaryBrokerInfo(); avail {
		res = &ClusterJoinResponse{
			PrimaryBroker: &BrokerInstance{
				Id:   id,
				Name: name,
				Addr: addr,
			},
			Status: joinStatusCode200,
		}

		// if this broker is the elected primary...
		var _resp *ForwardClusterJoinResponse
		_fwdRequest := &ForwardClusterJoinRequest{
			IsPrimaryEligible: false,
			Broker: &BrokerInstance{
				Id:   req.GetBrokerID(),
				Name: req.GetBrokerName(),
				Addr: req.GetBrokerAddr(),
			},
		}
		if strings.Compare(s.broker.GetBrokerID(), id) == 0 {
			_resp1, err2 := s.ForwardClusterJoin(ctx, _fwdRequest)
			if err2 != nil {
				s.log.Errorf("[InitiateClusterJoin self] forward to Elected Primary on cluster join request failed, reason: [%v]\n", err2)
				err = err2
				return
			}
			_resp = _resp1
		} else {
			// create connection to Primary and forward the request
			// should be fwd to the ELECTED primary... (unless this broker is the ONE)
			_gConn, err2 := grpc.Dial(addr, grpc.WithInsecure())
			if err2 != nil {
				err = err2
				s.log.Errorf("[InitiateClusterJoin fwd dial] forward to Elected Primary on cluster join request failed, reason: [%v]\n", err2)
				return
			}
			defer _gConn.Close()
			srv := NewMetastateServiceClient(_gConn)
			defer func() { srv = nil }()
			_resp1, err3 := srv.ForwardClusterJoin(ctx, _fwdRequest)
			if err3 != nil {
				err = err3
				s.log.Errorf("[InitiateClusterJoin fwd call] forward to Elected Primary on cluster join request failed, reason: [%v]\n", err3)
				return
			}
			_resp = _resp1
		}

		// handle the resposne
		if _resp.GetStatus() == fwdStatusCode200 {
			// OK update the res object - version brokermap
			res.Stateversion = _resp.GetStateversion()
			res.BrokersMap = _resp.GetBrokersMap()

		} else if _resp.GetStatus() == fwdStatusCode500 {
			// error
			res.Status = _resp.GetStatus()
		}

	} else if !avail {
		// in progress
		res = &ClusterJoinResponse{
			Status: joinStatusCode300,
		}
	}
	return
}

/* ----------------------------------------------- */
/*		cluster forming / joining APIs (client impl)	*/
/* ----------------------------------------------- */

// ClientInitClusterJoinRequest - initiate the join cluster request by non primary-eligible broker(s)
func (s *Service) ClientInitClusterJoinRequest() {
	_tBrokers := s.broker.GetBootstrapInitialPrimaryBrokersList()
	_gConns := make([]*grpc.ClientConn, len(_tBrokers))
	_srvs := make([]MetastateServiceClient, len(_tBrokers))
	defer func() {
		for _, _gConn := range _gConns {
			if _gConn != nil {
				_gConn.Close()
			}
		}
		_srvs = nil
	}()

	for true {
		// elected primary info ready - exit the endless join loop
		s.mux.Lock()
		if _id, _, _, _avail := s.GetElectedPrimaryBrokerInfo(); _avail {
			s.mux.Unlock()
			s.log.Infof("[ClientInitClusterJoinRequest] %v(%v) joined the cluster. Primary broker ID: %v\n",
				s.broker.GetBrokerName(), s.broker.GetBrokerAddr(), _id)
			s.log.Infof("[ClientInitClusterJoinRequest] available broker(s) %v\n",
				s.GetAvailableBrokersMap())
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
				_srv = NewMetastateServiceClient(_gConn)
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
				s.log.Debugf("[ClientInitClusterJoinRequest] rare case, could not connect to the target broker[%v], reason:[%v]\n", _tBroker, err)
				continue
			}

			s.log.Debugf("[ClientInitClusterJoinRequest] response returned [%v]\n", _resp)
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
				// primary's stateVersion and stateNum...
				s.UpsertInMem(KeyStateVersion, _resp.GetStateversion().GetStateVersion(), false)
				s.UpsertInMem(KeyStateVersionID, fmt.Sprintf("%v", _resp.GetStateversion().GetStateNum()), false)
				// update the elected primary information as well
				s.UpsertInMem(KeyPrimaryBrokerID, _resp.GetPrimaryBroker().GetId(), true)
				s.UpsertInMem(KeyPrimaryBrokerName, _resp.GetPrimaryBroker().GetName(), true)
				s.Upsert(KeyPrimaryBrokerAddr, _resp.GetPrimaryBroker().GetAddr(), true, false, true)

				s.mux.Unlock()
				break

			} else if _resp.Status == joinStatusCode300 {
				// in progress (continue to ping again till election done etc)
				s.log.Infof("[ClientInitClusterJoinRequest] waiting for an Elected Primary broker to present... retry initiated")
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

/* -------------------------------------------------------- */
/*    forward to Elected Primary request(s) (server impl)	*/
/* -------------------------------------------------------- */

// ForwardClusterJoin - from eligible brokers, forwarding to the Elected Primary broker for updates on meta-state
func (s *Service) ForwardClusterJoin(ctx context.Context, req *ForwardClusterJoinRequest) (res *ForwardClusterJoinResponse, err error) {
	res = &ForwardClusterJoinResponse{
		Status: fwdStatusCode200,
	}
	// add this BrokerMeta too (missing or new)
	_brokersMap := s.GetAvailableBrokersMap()
	if IsBrokerMetaStructNil(s.inMemStates[KeyAvailableBrokers].(map[string]BrokerMeta)[req.GetBroker().GetId()]) {
		s.inMemStates[KeyAvailableBrokers].(map[string]BrokerMeta)[req.GetBroker().GetId()] = BrokerMeta{
			id:                req.GetBroker().GetId(),
			name:              req.GetBroker().GetName(),
			addr:              req.GetBroker().GetAddr(),
			isPrimaryEligible: req.GetIsPrimaryEligible(),
		}
	}
	// brokersMap serialization
	_brokerMetaList := make([]string, 0)
	_brokersMap = s.GetAvailableBrokersMap() // get it again to make sure all brokers are added (the above missing one)
	for _, _bMeta := range _brokersMap {
		_brokerMetaList = append(_brokerMetaList, _bMeta.SerializeToString())
	}
	res.BrokersMap = _brokerMetaList

	// update the state version as well...
	if err2 := s._persist(true); err2 != nil {
		s.log.Errorf("[ForwardClusterJoin] update state version failed, reason: %v\n", err2)
		res.Status = fwdStatusCode500
		err = err2
		return
	}

	// res update state version
	_stateNum, err := strconv.Atoi(s.GetStateVersionID().(string))
	res.Stateversion = &StateVersionInfo{
		StateVersion: s.GetStateVersion(),
		StateNum:     int32(_stateNum),
	}
	return
}

// BroadcastMetaStateUpdates - broadcast meta state updates to targeted brokers
// (basically to primary eligible brokers, as they are backups for being primary election when necessary)
// (exception is when the primary broker has been re-elected; then all available brokers MUST be broadcasted / informed)
func (s *Service) BroadcastMetaStateUpdates(ctx context.Context, req *BroadcastRequest) (res *BroadcastResponse, err error) {
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

/* -------------------- */
/*		utility func		*/
/* -------------------- */

func (s *Service) _updateElectionWonInMemStates(brokerID, brokerName, brokerAddr string) {
	s.UpsertInMem(KeyPrimaryBrokerID, brokerID, true)
	s.UpsertInMem(KeyPrimaryBrokerName, brokerName, true)
	s.Upsert(KeyPrimaryBrokerAddr, brokerAddr, true, true, true)
}

// create a random timer to throttle the ping / dial / connection
func (s *Service) _getRandomTimerForJoinCluster() (timer *time.Timer) {
	_seed := rand.NewSource(time.Now().UnixNano())
	_r := rand.New(_seed)
	// at least 3 sec, max within 4 sec
	// (intervalJoinClusterDial/4 -> 1000ms; intervalJoinClusterDial/5*3 -> 3000ms = sum is roughly within 4000ms)
	_randomInterval := _r.Intn(intervalJoinClusterDial/5) + intervalJoinClusterDial/5*3
	s.log.Debugf("[_getRandomTimerForJoinCluster] TBD timer interval: [%v]\n", _randomInterval)
	timer = time.NewTimer(time.Duration(_randomInterval) * time.Millisecond)

	return
}

// _updateToElectedMasterState - method to update the "is_primary_broker" state
func (s *Service) _updateToElectedMasterState() {
	_, err := s.Upsert(KeyPrimaryBroker, true, true, true, true)
	if err != nil {
		panic(fmt.Errorf("[_updateToElectedMasterState] set meta state exception, reason: %v", err))
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
