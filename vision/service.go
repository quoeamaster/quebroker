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
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Service - service implementation for the gRPC call
type Service struct {
	log *logrus.Logger // logger instance
}

// New - create a new Service instance
func New() (s *Service) {
	s = new(Service)
	s.setupLogger()
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

// GetVision - implementation of API
func (s *Service) GetVision(ctx context.Context, in *Request) (out *Response, err error) {
	out = new(Response)

	// TODO: add back real implementation
	switch in.Api {
	case APIBrokersVision:
		out.Code = 200
		out.Payload = "{ \"msg\": \"testing only~ completed\" }"
	default:
		out.Code = 500
		out.Payload = fmt.Sprintf("{ \"error\": \"unknown API [%v]\" }", in.Api)
	}
	return
}
