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
package vision

import (
	"strings"
	"testing"

	proto "github.com/golang/protobuf/proto"
)

func TestMarshalCapabilitiesOnProtoBuf(t *testing.T) {
	_h1 := "[TestMarshalCapabilitiesOnProtoBuf]"
	tests := []struct {
		api     string
		format  string
		verbose bool
	}{
		{APIBrokersVision, PayloadFormatJSON, true},
		{APIPartitionsVision, PayloadFormatText, false},
		{APIPartitionsVision, PayloadFormatJSON, false},
		{APIPartitionsVision, PayloadFormatJSON, true},
	}

	for _, test := range tests {
		_req := new(Request)
		_req.Api = test.api
		_req.Format = test.format
		_req.Verbose = test.verbose

		bContent, err := proto.Marshal(_req)
		if err != nil {
			t.Errorf("%v exception during marshal operation [%v]\n", _h1, err)
		} else {
			// wire format byte[] (os independent)
			t.Logf("%v data in bytes: [%v], length => [%v]\n", _h1, bContent, len(bContent))
			// [DOC] marsha as textString returns something like field: "value" (e.g. api: "partitions.visions")
			//t.Logf("%v in string format: [%v]", _h1, proto.MarshalTextString(_req))
		}
		// test unMarshal
		_unReq := new(Request)
		err = proto.Unmarshal(bContent, _unReq)
		if err != nil {
			t.Errorf("%v exception during un-marshal operation [%v]\n", _h1, err)
		} else {
			if strings.Compare(_req.GetApi(), _unReq.GetApi()) != 0 ||
				strings.Compare(_req.GetFormat(), _unReq.GetFormat()) != 0 ||
				_req.GetVerbose() != _unReq.GetVerbose() {
				t.Errorf("%v expected [%v] BUT got [%v]\n", _h1, _req, _unReq)
			}
			//t.Logf("%v data unmarshal: api[%v], format[%v], verbose[%v]\n", _h1, _unReq.GetApi(), _unReq.GetFormat(), _unReq.GetVerbose())
		}
	}
	// PS. you can NOT marshal a non ProtoBuf supported data type (e.g. an ordinary map or slice)
}
