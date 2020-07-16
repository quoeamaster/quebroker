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
	"os/signal"
	"syscall"
	"time"

	"github.com/quoeamaster/quebroker"
	"github.com/quoeamaster/quebroker/metastate"
	"github.com/quoeamaster/quebroker/vision"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// NetTCP - network protocol TCP
const NetTCP = "tcp"

var log = quebroker.GetBasicTextLogger() // TODO: add a file logger too

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
	err = _setupServiceBindings(gRPCServer, broker)
	if err != nil {
		return
	}

	// y. start signal monitoring on terminate or interrupt
	_signals := make(chan os.Signal, 1)
	signal.Notify(_signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		_sig := <-_signals
		log.Infof("[signal received] %v", _sig)
		// received either INTERRUPT or TERMINATE signal; gracefully exit
		defer broker.Stop()
		// [DOC] closing gRPCServer = closing tcpListener as well
		/*
			defer func() {
				err2 := tcpListener.Close()
				if err2 != nil {
					err = fmt.Errorf("could not close network connector [tcp], reason: %v", err2)
					return
				}
			}()
		*/
		defer gRPCServer.GracefulStop()
	}()

	// e. bootstrap services (related)
	log.Infof("[bootstrap broker] address: %v", tcpListener.Addr().String())
	go func() {
		log.Infof("[engine] forming cluster...")
		_timer := time.NewTimer(1200 * time.Millisecond)
		<-_timer.C
		_clusterForming(broker)
	}()

	// z. start to serve (with all services registered)
	err1 = gRPCServer.Serve(tcpListener)
	if err1 != nil {
		err = fmt.Errorf("failed to start up the RPC server, reason %v", err1)
		return
	}
	return
}

// _clusterForming - boostrap a multiple broker architecture (forming and joining cluster)
func _clusterForming(broker *quebroker.Broker) {
	// PS. panic directly if met fatal exceptions
	_t := time.NewTimer(2 * time.Second)
	<-_t.C
	// determine which process to employ
	// a. if eligible for election => ClientInitElectionRequest()
	// b. else => ClientInitClusterJoinRequest()
	if broker.GetIsPrimaryCandidate() {
		broker.MetaState.ClientInitElectionRequest()
	} else {
		broker.MetaState.ClientInitClusterJoinRequest()
	}
}

// methd to setup all service bindings to the gRPC server
func _setupServiceBindings(gRPCServer *grpc.Server, broker *quebroker.Broker) (err error) {
	// TODO: add back new service bindings
	// vision
	vision.RegisterVisionServiceServer(gRPCServer, broker.Vision)
	log.WithFields(logrus.Fields{"vision": "service to retrieve stats of the broker"}).Info("[service registered]")

	// metastate
	metastate.RegisterMetastateServiceServer(gRPCServer, broker.MetaState)
	log.WithFields(logrus.Fields{"metastate": "service to handle cluster state management + cluster formation"}).Info("[service registered]")

	return
}

// setupLoggers - method to setup logger configs
func setupLoggers() {
	// default logger (additional settings if any)

	// TODO: file-logger
}

func main() {
	setupLoggers()

	// start up server
	// _broker, _tcpListener, _gRPCServer, err := _startServer()
	_, _, _, err := _startServer()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("[broker stopped]")
}
