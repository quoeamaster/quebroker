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

package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/quoeamaster/quebroker"
	"github.com/quoeamaster/quebroker/vision"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// NetTCP - network protocol TCP
const NetTCP = "tcp"

var log = logrus.New() // TODO: add a file logger too

func _setupSignals() {

}

// _startServer - method to start the gRPC server and bind services to it
func _startServer() (broker *quebroker.Broker, tcpListener net.Listener, gRPCServer *grpc.Server, err error) {
	// a. load broker config
	broker, err1 := quebroker.BrokerInstanceFromTomlConfig()
	if err1 != nil {
		err = fmt.Errorf("could not load the broker configuration, reason :%v", err1)
		return
	}
	//log.WithField("broker.config", broker.String()).Info("[configuration loaded]")
	log.Infof("[configuration loaded] %v", broker.String())

	// b. open TCP socket connection
	tcpListener, err1 = net.Listen(NetTCP, fmt.Sprintf("%v:%v", broker.Network.HostName, broker.Network.Port))
	if err1 != nil {
		err = fmt.Errorf("failed to get network connector [tcp], reason %v", err1)
		return
	}

	// c. create gRPC server (kind of service bus / hub)
	gRPCServer = grpc.NewServer()

	// d. add back service bindings
	vision.RegisterVisionServiceServer(gRPCServer, broker)
	log.WithFields(logrus.Fields{"vision": "service to retrieve stats of the broker"}).Info("[service registered]")

	// z. start to serve (with all services registered)
	log.Infof("[bootstrap broker] address: %v", tcpListener.Addr().String())
	err1 = gRPCServer.Serve(tcpListener)
	if err1 != nil {
		err = fmt.Errorf("failed to start up the RPC server, reason %v", err1)
		return
	}

	return
}

// setupLoggers - method to setup logger configs
func setupLoggers() {
	// default logger
	log.Out = os.Stderr
	// per logger can have a different textFormatter -> for time format.. check the below
	// https://golang.org/pkg/time/
	log.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})

	// TODO: file-logger
}

func main() {
	setupLoggers()

	// setup signal hooks
	_setupSignals()

	// start up server
	_broker, _tcpListener, _gRPCServer, err := _startServer()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Broker instance configs: [%v]\n", _broker)
	//log.Printf("listener: %v\n", _tcpListener.Addr().String())
	//log.Printf("gRPC server: %v\n", _gRPCServer)

	defer _tcpListener.Close()
	defer _gRPCServer.Stop()

	defer log.Infof("[stopping broker] exit sequence initiated...")

	// TODO: handle os signal term?
}
