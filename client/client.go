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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const payloadFormatJSON = "json" // payload's final presentation format in json
const payloadFormatText = "text" // payload's final presentation format in plain text

// Client - Object acting as the interface to connect with the gRPC server
type Client struct {
	log         *logrus.Logger // logger instance
	hostname    string         // connecting server's hostname
	port        int            // connecting server's port
	clusterName string         // target cluster's name
	// TODO: add seed hosts instead of 1 single hostname

	conn *grpc.ClientConn // tcp socket connecting to gRPC server

	// TODO: add back the servceImpl instead of direct connection
	Vision *VisionService // service for vision api(s)
}

// New - new a Client instance
func New() (instance *Client) {
	instance = new(Client)
	instance.setupLogger()
	instance.log.Debugf("[client bootstrap] %v\n", instance)

	return
}

// setupLogger - setup the per instance logger
func (c *Client) setupLogger() {
	c.log = logrus.New()
	c.log.Out = os.Stderr
	c.log.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})
}

// Connect - connect to the gRPC server
func (c *Client) Connect() (err error) {
	defer func() {
		// any unexpected panic(s), recovered and transform to an Error
		if _r := recover(); _r != nil {
			err = _r.(error)
		}
	}()
	c.conn, err = grpc.Dial(fmt.Sprintf("%v:%v", c.hostname, c.port), grpc.WithInsecure())
	if err != nil {
		return
	}

	// setup service(s)
	c.Vision, err = NewVisionService(c.conn)
	if err != nil {
		return
	}
	// TODO: update this when more services available

	return
}

// Close - exit the client sequence
func (c *Client) Close() (err error) {
	if c.conn != nil {
		if err = c.conn.Close(); err != nil {
			return
		}
	}
	return
}

// --------------------- //
// ***	setter(s)	*** //
// --------------------- //

// SetHostname - set the hostname for connection
func (c *Client) SetHostname(hostname string) (err error) {
	if strings.Trim(hostname, "") != "" {
		c.hostname = hostname
	} else {
		err = fmt.Errorf("hostname is invalid [%v]", hostname)
	}
	return
}

// SetPort - set the port for connection
func (c *Client) SetPort(port int) (err error) {
	if port > 0 {
		c.port = port
	} else {
		err = fmt.Errorf("port is invalid [%v]", port)
	}
	return
}

// SetClusterName - declare the clusterName for connection
func (c *Client) SetClusterName(clusterName string) (err error) {
	if strings.Trim(clusterName, "") != "" {
		c.clusterName = clusterName
	} else {
		err = fmt.Errorf("clusterName is invalid [%v]", clusterName)
	}
	return
}
