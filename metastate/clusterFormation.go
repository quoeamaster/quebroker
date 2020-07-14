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
	return
}

func (s *Service) ClientInitClusterJoinRequest() {}

/* -------------------------------------- */
/*		response status-code translator		*/
/* -------------------------------------- */

// _electionDoneACKRespStatusTranslator - return the translated message based on status-code
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
