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
	"log"
	"net"

	"github.com/quoeamaster/quebroker"
	"google.golang.org/grpc"
)

// NetTCP - network protocol TCP
const NetTCP = "tcp"

func _startServer() (broker *quebroker.Broker, tcpListener net.Listener, gRPCServer *grpc.Server, err error) {
	// a. load broker config
	broker, err1 := quebroker.BrokerInstanceFromTomlConfig()
	if err1 != nil {
		err = fmt.Errorf("could not load the broker configuration, reason :%v", err1)
	}

	// b. open TCP socket connection
	tcpListener, err1 = net.Listen(NetTCP, fmt.Sprintf("%v:%v", broker.Network.HostName, broker.Network.Port))
	if err1 != nil {
		err = fmt.Errorf("failed to start up server, reason %v", err1)
	}

	// c. create gRPC server (kind of service bus / hub)
	gRPCServer = grpc.NewServer()

	// d. add back bindings with services

	return
}

func main() {
	// start up server
	_broker, _tcpListener, _gRPCServer, err := _startServer()
	if err != nil {
		log.Fatalf("failed to start up server, reason %v\n", err)
	}
	log.Printf("Broker instance configs: [%v]\n", _broker)
	log.Printf("listener: %v\n", _tcpListener.Addr().String())
	log.Printf("gRPC server: %v\n", _gRPCServer)

	defer _tcpListener.Close()

	// TODO: handle os signal term?
}
