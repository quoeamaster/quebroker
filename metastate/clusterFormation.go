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
	"encoding/json"
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
	// must init a valid instance of Dummy struct
	res = &Dummy{}

	// add the logic to check whether Primary Broker has already been ELECTED before doing the ID comparison
	// if priamry available => do a broadcast instead (since every [ hm... ?? primary eligible] broker will know you have joined)
	if _, _, _, avail := s.GetElectedPrimaryBrokerInfo(); avail {
		// update the brokersMap to add in this instance too
		s._upsertBrokerToBrokersMap(req.BrokerID, req.BrokerName, req.BrokerAddr, true)
		// update version state as well since change in brokersMap content
		s._persist(true)

		s.log.Info("[InitiateElectionRequest] broadcast sent to brokers since Primary Broker already ELECTED")
		s.initBroadcastMetaStateUpdates(false, false)
		// throttle the election interval (to reduce number of unecessary broadcast)
		<-time.NewTimer(intervalElectionDial * time.Millisecond).C
		return
	}

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
		// [QQ] trigger the broadcast here after a throttle e.g. 2 seconds~~
		// no need since the upper Primary Broker Check would also trigger broadcast

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
		//s.log.Warnf("[InitiateElectionRequest] ** srv created , before calling GetACK %v, req [%v], current broker ID, name %v, %v\n", _srv, req, s.broker.GetBrokerID(), s.broker.GetBrokerName())
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
		//s.log.Warnf("[InitiateElectionRequest] ACK response %v\n", _resp)
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
	s.log.Infof("[InitiateElectionRequest] finally ... state [%v] vs in-mem state [%v], available broker map: {%v}\n",
		s.states, s.inMemStates, s.GetAvailableBrokersMap())
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
		s.log.Debugf("[GetElectedPrimaryACK] updated brokers list %v\n", s.GetAvailableBrokersMap())
	}
	// a2. is this instance's elected primary attributes set?
	if _, _, _, avail := s.GetElectedPrimaryBrokerInfo(); !avail {
		s.mux.Lock()
		// this broker instance is winner (for sure); only winner can send out ACK (state version also updated)
		s.UpsertInMem(KeyPrimaryBroker, true, true)
		s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())
		s.log.Debugf("[GetElectedPrimaryACK] updated winner broker to Elected primary")

		s.mux.Unlock()
	}

	// b. return ACK Response
	_stateNum, err := strconv.Atoi(s.GetStateVersionID().(string))
	res = &ElectionDoneHandshakeACKResponse{
		Code:         int32(ackStatusCode200),
		StateVersion: s.GetStateVersion(),
		StateNum:     int32(_stateNum),
	}
	//s.log.Warnf("[GetElectedPrimaryACK] ** %v\n", res)

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
		s.log.Infof("[ClientInitElectionRequest] Primary Election Done: {%v}\n", s._infoLogBrokersStatus())
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
		s.log.Infof("[ClientInitElectionRequest] Primary Election Done: {%v}\n", s._infoLogBrokersStatus())
		return
	}

	// start the request polling
	//_srvs := make([]MetastateServiceClient, len(_targetBrokers))
	//_gConns := make([]*grpc.ClientConn, len(_targetBrokers))
	_srvs := make(map[string]MetastateServiceClient)
	_gConns := make(map[string]*grpc.ClientConn)
	defer func() {
		for _, _gConn := range _gConns {
			if _gConn != nil {
				if err2 := _gConn.Close(); err2 != nil {
					s.log.Warnf("[ClientInitElectionRequest] try to close the corresponding grpc connection, but got exception with reason: [%v]\n", err2)
				}
			}
		}
		_gConns = nil
		_srvs = nil
	}()
	_retry := 0
	for true {
		// primary elected + ACK received
		s.mux.Lock()
		if _, _, _, _found := s.GetElectedPrimaryBrokerInfo(); _found {
			s.mux.Unlock()
			s.log.Infof("[ClientInitElectionRequest] Primary Election Done: {%v}\n", s._infoLogBrokersStatus())
			break
		}
		s.mux.Unlock()

		// extreme case, no other broker's available for ping / dial / connection
		// so the local broker is the Elected Primary
		if _retry >= maxElectionDialRetrial {
			s._updateToElectedMasterState()
			s._updateElectionWonInMemStates(s.broker.GetBrokerID(), s.broker.GetBrokerName(), s.broker.GetBrokerAddr())
			s.log.Infof("[ClientInitElectionRequest] Primary Election Done: {%v}\n", s._infoLogBrokersStatus())
			break
		}

		// _tBroker = the target broker's address
		for _, _tBroker := range _targetBrokers {
			// create connection and store it for re-use
			_srv := _srvs[_tBroker]
			if _srv == nil {
				_gConn, err := grpc.Dial(_tBroker, grpc.WithInsecure())
				if err != nil {
					s.log.Warnf("[ClientInitElectionRequest] could not connect with [%v]\n", _tBroker)
					continue
				}
				_srv = NewMetastateServiceClient(_gConn)
				_srvs[_tBroker] = _srv
				_gConns[_tBroker] = _gConn // required... as you need to clean up the ClientConn after election done
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
			s.log.Infof("[InitiateClusterJoin self] forward to itself the cluster join operations\n")
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
			s.log.Infof("[InitiateClusterJoin fwd] forward to [%v] the cluster join operations\n", addr)
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

			// need to self update? nah... broadcast is coming

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
	//_gConns := make([]*grpc.ClientConn, len(_tBrokers))
	//_srvs := make([]MetastateServiceClient, len(_tBrokers))
	_gConns := make(map[string]*grpc.ClientConn)
	_srvs := make(map[string]MetastateServiceClient)
	defer func() {
		for _, _gConn := range _gConns {
			if _gConn != nil {
				_gConn.Close()
			}
		}
		_gConns = nil
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
		for _, _tBroker := range _tBrokers {
			_srv := _srvs[_tBroker]
			if _srv == nil {
				_gConn, err := grpc.Dial(_tBroker, grpc.WithInsecure())
				if err != nil {
					s.log.Warnf("[ClientInitClusterJoinRequest] connection error, reason: %v\n", err)
					continue
				}
				_srv = NewMetastateServiceClient(_gConn)
				_gConns[_tBroker] = _gConn
				_srvs[_tBroker] = _srv
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
				s.log.Infof("[ClientInitClusterJoinRequest] waiting for target broker [%v] to be availabe... initiate retry", _tBroker)
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
	s._upsertBrokerToBrokersMap(req.GetBroker().GetId(), req.GetBroker().GetName(), req.GetBroker().GetAddr(), req.GetIsPrimaryEligible())
	// brokersMap serialization
	_brokersMap := s.GetAvailableBrokersMap()
	_brokerMetaList := make([]string, 0)
	for _, _bMeta := range _brokersMap {
		_brokerMetaList = append(_brokerMetaList, _bMeta.SerializeToString())
	}
	res.BrokersMap = _brokerMetaList

	s.log.Tracef("[ForwardClusterJoin] ** b4 persist state %v-%v\n", s.GetStateVersion(), s.GetStateVersionID())
	// update the state version as well...
	if err2 := s._persist(true); err2 != nil {
		s.log.Errorf("[ForwardClusterJoin] update state version failed, reason: %v\n", err2)
		res.Status = fwdStatusCode500
		err = err2
		return
	}
	s.log.Tracef("[ForwardClusterJoin] ** AFTER persist state %v-%v = %v\n", s.GetStateVersion(), s.GetStateVersionID(), s.statePathOnDisk)

	// res update state version
	_stateNum, err := strconv.Atoi(s.GetStateVersionID().(string))
	res.Stateversion = &StateVersionInfo{
		StateVersion: s.GetStateVersion(),
		StateNum:     int32(_stateNum),
	}

	// let Primary broker broadcast instead (since the most update info should be there)
	// - update joining broker into brokerMap as well
	// - broadcast
	s.initBroadcastMetaStateUpdates(false, false)

	return
}

// BroadcastMetaStateUpdates - broadcast meta state updates to targeted brokers
// (basically to primary eligible brokers, as they are backups for being primary election when necessary)
// (exception is when the primary broker has been re-elected; then all available brokers MUST be broadcasted / informed)
func (s *Service) BroadcastMetaStateUpdates(ctx context.Context, req *BroadcastRequest) (res *BroadcastResponse, err error) {
	// check if the meta states are stale or up-to-date; stale / old states should be IGNORED
	_localStateVerNumRaw := s.GetStateVersionID()
	if _localStateVerNumRaw != nil {
		_localStateVerNum, err1 := strconv.Atoi(_localStateVerNumRaw.(string))
		if err1 != nil {
			s.log.Warnf("[BroadcastMetaStateUpdates] could not convert local State ID, reason: %v\n", err)
		} else {
			if req.Stateversion.GetStateNum() <= int32(_localStateVerNum) {
				s.log.Infof("[BroadcastMetaStateUpdates] stale / less update state encountered, skip this update")
				// MUST create an empty response before returning (nil would cause exception)
				res = &BroadcastResponse{
					Status: broadcastStatusCode200,
				}
				err = err1
				return
			}
		}
	}

	s.log.Debugf("[BroadcastMetaStateUpdates] received broadcast update [%v] - latest state version [%v - %v]\n",
		s.broker.GetBrokerAddr(), req.Stateversion.GetStateVersion(), req.Stateversion.GetStateNum())
	s.mux.Lock()
	defer s.mux.Unlock()

	// original primary broker info (on failure reset)
	_id, _name, _addr, _avail := s.GetElectedPrimaryBrokerInfo()
	res = new(BroadcastResponse)
	res.Status = broadcastStatusCode200

	// a1. determine what to update; re-elected primary OR
	// a2. is primary info NOT exists in this broker???
	if req.GetIsPrimaryReElected() || !_avail {
		s.log.Debugf("[BroadcastMetaStateUpdates] need to update primary broker info: broker info exists? => %v OR is a New Primary broker elected? => %v\n", _avail, req.GetIsPrimaryReElected())
		s._updateReElectedPrimaryInMem(req.GetPrimaryBroker().GetId(), req.GetPrimaryBroker().GetName(), req.GetPrimaryBroker().GetAddr(), false)
	}

	// b. update also the meta states
	_mState := make(map[string]interface{})
	err = json.Unmarshal([]byte(req.GetMetaStateInJson()), &_mState)
	if err != nil {
		s._updateReElectedPrimaryInMem(_id, _name, _addr, false)
		res.Status = broadcastStatusCode500
		return
	}
	// TODO: check whether which part of the states should be modified...
	s.inMemStates[KeyAvailableBrokers] = _mState[KeyAvailableBrokers]

	// c. update the state's version
	s.Upsert(KeyStateVersion, req.Stateversion.GetStateVersion(), false, false, false)
	s.Upsert(KeyStateVersionID, fmt.Sprintf("%v", req.Stateversion.GetStateNum()), true, false, false)

	return
}

/* -------------------------------------------------------- */
/*    forward to Elected Primary request(s) (client impl)	*/
/* -------------------------------------------------------- */

// initBroadcastMetaStateUpdates -
func (s *Service) initBroadcastMetaStateUpdates(isPrimaryEligiblesOnly bool, isPrimaryReElected bool) {
	// a. only Elected Primary can do such broadcast
	if _, _, _, avail := s.GetElectedPrimaryBrokerInfo(); !avail {
		s.log.Infof("[initBroadcastMetaStateUpdates] cluster not ready yet, Primary Election not yet done")
		return
	} else if isP := s.inMemStates[KeyPrimaryBroker]; isP != nil && isP.(bool) == false {
		s.log.Warnf("[initBroadcastMetaStateUpdates] trying to init a broadcast on a NON elected primary broker %v\n", s.broker.GetBrokerID())
		return
	}
	// b. prepare the broadcast target broker addr(s) //tBrokers := make([]string, 0)
	//s.log.Info("[initBroadcastMetaStateUpdates] *** seems brokersMap is empty: ", s.GetAvailableBrokersMap(), " states: ", s.states, " ", s.inMemStates)
	for _, tBroker := range s.GetAvailableBrokersMap() {
		_needBCast := false
		if strings.Compare(tBroker.GetID(), s.broker.GetBrokerID()) == 0 {
			// ignore self broadcast, does not make sense...
			continue
		}
		if isPrimaryEligiblesOnly && tBroker.IsPrimaryEligible() {
			_needBCast = true
		} else {
			_needBCast = true
		}
		if _needBCast {
			tBrokerAddr := tBroker.GetAddr()
			tBrokerID := tBroker.GetID()
			// goroutine / thread
			go func() {
				s.log.Debugf("[initBroadcastMetaStateUpdates] target [%v]\n", tBrokerAddr)

				_isOffline, err1 := s._broadcastToTargetBroker(tBrokerAddr, isPrimaryReElected)
				if err1 != nil {
					s.log.Warnf("[initBroadcastMetaStateUpdates] target broker (%v) cannot be connected, reason: %v\n", tBrokerAddr, err1)
				}
				// marke offline => would trigger another round of broadcast though
				if _isOffline {
					s.markOfflineForBroker(tBrokerID, tBrokerAddr)
				}
			}()
		}
	}

}

// _broadcastToTargetBroker - method to broadcast to a list of broker(s)
func (s *Service) _broadcastToTargetBroker(addr string, isPrimaryReElected bool) (isBrokerOffline bool, err error) {
	_gConn, err := grpc.Dial(addr, grpc.WithInsecure())
	defer _gConn.Close()
	if err != nil {
		return
	}
	srv := NewMetastateServiceClient(_gConn)

	// prepare params
	var _pBroker *BrokerInstance
	if id, name, addr, avail := s.GetElectedPrimaryBrokerInfo(); avail {
		_pBroker = &BrokerInstance{
			Id:   id,
			Name: name,
			Addr: addr,
		}
	}
	bMetaJSON, err := json.Marshal(s.states)
	if err != nil {
		return
	}
	bInMemMetaJSON, err := json.Marshal(s.inMemStates)
	if err != nil {
		return
	}
	_stateV := new(StateVersionInfo)
	_stateV.StateVersion = s.GetStateVersion()
	_sNum, err := strconv.Atoi(s.GetStateVersionID().(string))
	_stateV.StateNum = int32(_sNum)

	_bcastSuccess := false
	// 3 times of retrial (each retrial is 1.5 seconds) roughly within 5 seconds will know if a broker is offline or not
	for i := 0; i < maxBroadcastRetrial; i++ {
		s.log.Debugf("[_broadcastToTargetBroker] *** b4 calling grpc BroadcastMetaStateUpdates %v - trial: %v\n", addr, i)
		_resp, err := srv.BroadcastMetaStateUpdates(context.Background(), &BroadcastRequest{
			IsPrimaryReElected:   isPrimaryReElected,
			PrimaryBroker:        _pBroker,
			MetaStateInJson:      string(bMetaJSON),
			InMemMetaStateInJson: string(bInMemMetaJSON),
			Stateversion:         _stateV,
		})
		if err != nil {
			// connectivity issue maybe
			s.log.Warnf("[_broadcastToTargetBroker] broadcast failed, reason: %v\n", err)
			// throttle 1.5s
			<-time.NewTimer(intervalBroadcastDial * time.Millisecond).C
			continue
		}
		// check response
		if _resp.Status == broadcastStatusCode200 {
			_bcastSuccess = true
			break
		} else {
			// error of any type
			s.log.Warnf("[_broadcastToTargetBroker] broadcast failed with status %v\n", _resp.Status)
			// retry - throttle 1.5s
			<-time.NewTimer(intervalBroadcastDial * time.Millisecond).C
			continue
		}
	}
	// at least broadcast once with success?
	if !_bcastSuccess {
		isBrokerOffline = true
		err = fmt.Errorf("[_broadcastToTargetBroker] target broker (%v) is not reachable or broadcast failed to it, MARK-OFFLINE", addr)
	}
	return
}

func (s *Service) markOfflineForBroker(brokerID string, brokerAddr string) (err error) {
	// mark offline would trigger another broadcast though

	s.log.Warnf("*** TBD targeted broker: %v - %v", brokerID, brokerAddr)

	// TODO: receiver side would need to compare the state version number and decide to update or not (stale state due to network latency)
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

// update primary broker inform in in-mem state; depends on needPersist param to persist
// no stateVersion updated AND changes are only to the in-mem state (not persistable state)
func (s *Service) _updateReElectedPrimaryInMem(brokerID, brokerName, brokerAddr string, needPersist bool) {
	s.UpsertInMem(KeyPrimaryBrokerID, brokerID, true)
	s.UpsertInMem(KeyPrimaryBrokerName, brokerName, true)
	s.Upsert(KeyPrimaryBrokerAddr, brokerAddr, needPersist, false, true)
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

func (s *Service) _upsertBrokerToBrokersMap(id, name, addr string, isPrimaryEligible bool) {
	_m := s.GetAvailableBrokersMap()
	_m[id] = BrokerMeta{
		id:                id,
		name:              name,
		addr:              addr,
		isPrimaryEligible: isPrimaryEligible,
	}
}

// _infoLogBrokersStatus - method to print out the primary and other broker(s) nicely
func (s *Service) _infoLogBrokersStatus() string {
	var _s strings.Builder
	_d := ": "
	_m := s.GetAvailableBrokersMap()
	id, name, addr, avail := s.GetElectedPrimaryBrokerInfo()
	if avail {
		_s.WriteString("[PRIMARY]")
		_b := BrokerMeta{id: id, name: name, addr: addr, isPrimaryEligible: true}
		_s.WriteString(_b.SerializeToString())
		_s.WriteString(_d)
		// add the primary back to the brokers map if missing
		if IsBrokerMetaStructNil(_m[id]) {
			_m[id] = _b
		}
	}
	// rest of the brokers
	_i := 0
	for _, _b := range _m {
		// excluding primary
		if strings.Compare(_b.id, id) != 0 {
			if _i > 0 {
				_s.WriteString(_d)
			}
			_s.WriteString(_b.SerializeToString())
		}
		_i++
	}
	_log := _s.String()
	//s.log.Infof("available brokers {%v}\n", _log)
	return _log
}
