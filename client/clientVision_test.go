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
package client

import (
	"testing"
)

// TestConnectivity - test connectivity with the gRPC server and try to execute a Vision service query
// 	main goal is test connectivity and is an optional test-case
func TestConnectivity(t *testing.T) {
	_h1 := "[TestVisionGet]"
	t.Logf("%v main goal is to test the connectivity, hence can be skipped.\n", _h1)
	// TODO: skip this test before GA release (unless developer is using localhost:6801 as the primary broker)
	//t.Skip()

	_clientP := New()
	_clientP.SetHostname("localhost")
	_clientP.SetPort(6801)
	_clientP.SetClusterName("devCluster")

	err := _clientP.Connect()
	if err != nil {
		t.Errorf("%v failed to connect, reason: %v\n", _h1, err)
		t.FailNow()
	}
	defer func() {
		// close
		err = _clientP.Close()
		if err != nil {
			t.Errorf("%v failed to close broker, reason: %v\n", _h1, err)
			t.FailNow()
		}
	}()

	// invoke Vision.Brokers api
	_resp, err := _clientP.Vision.BrokersVision(payloadFormatJSON, false)
	if err != nil {
		t.Errorf("%v failed to run Broker.vision API, reason: %v\n", _h1, err)
		t.FailNow()
	}
	t.Logf("%v response returned! code [%v], payload [%v]", _h1, _resp.Code, _resp.Payload)
}
