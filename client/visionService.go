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
	"os"
	"strings"
	"time"

	"github.com/quoeamaster/quebroker/vision"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// VisionService - service wrapper for gRPC vision api(s)
type VisionService struct {
	log  *logrus.Logger
	conn vision.VisionServiceClient
}

// NewVisionService - create an instance of the Service (wrapper)
func NewVisionService(tcpSocket *grpc.ClientConn) (srv *VisionService, err error) {
	srv = new(VisionService)
	srv.setupLogger()
	srv.conn = vision.NewVisionServiceClient(tcpSocket)

	return
}

// setupLogger - setup the logger
func (v *VisionService) setupLogger() {
	v.log = logrus.New()
	v.log.Out = os.Stderr
	v.log.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})
}

// --------------------------- //
// ***	api interface		*** //
// --------------------------- //

// _createRequestByAPI - return a basic Request object for the gRPC call
func (v *VisionService) _createRequestByAPI(api string, format string, verbose bool) (request *vision.Request) {
	request = new(vision.Request)

	request.Api = api
	request.Verbose = verbose
	// format checks
	if strings.Trim(format, "") == "" {
		request.Format = payloadFormatJSON
	} else {
		// TODO: add validation rules on whether format is KNOWN format or not?
		request.Format = format
	}
	return
}

// BrokersVision - return broker(s) relation vision / status
func (v *VisionService) BrokersVision(format string, verbose bool) (response *vision.Response, err error) {
	_req := v._createRequestByAPI("brokers.vision", format, verbose)
	response, err = v.conn.GetVision(context.Background(), _req)
	return
}
