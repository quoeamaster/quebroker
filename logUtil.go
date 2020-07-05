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

package quebroker

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// GetBasicTextLogger - method to return a basic text logger
// 	timestamp enabled with RFC3339 format (have timezone)
func GetBasicTextLogger() (logger *logrus.Logger) {
	// per logger can have a different textFormatter -> for time format.. check the below
	// https://golang.org/pkg/time/
	logger = logrus.New()
	logger.Out = os.Stderr
	logger.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})

	return
}
