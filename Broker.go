/*
 *  Copyright Project - queBroker, Author - quoeamaster, (C) 2018
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
    "queutil"
    "github.com/emicklei/go-restful"
    "os"
    "os/signal"
    "syscall"
    "fmt"
    "sync"
    "net/http"
    "strings"
    "runtime"
)

// when the broker is not yet started up; this file indicated the broker's UUID
const brokerIdFile = "broker.id"

// after the broker started, the original broker.id file is renamed as
// broker.id.lock and "locked" up for updating
const brokerIdLockFile = "broker.id.lock"

// default log file name => quebroker.log
const brokerLogFilename = "quebroker.log"

// lock for the Broker singleton creation
var syncLock sync.Once
var brokerInstance *Broker


type Broker struct {
    // UUID for the broker (must be unique within a cluster)
    UUID string

    // broker config wrapper
    config *BrokerConfig

    // the locked broker.id.lock file
    //lockedBrokerIdFile lockfile.Lockfile

    // container for the RESTful-abled server
    webserver *restful.Container

    // logger for the Broker
    logger *queutil.FlexLogger

    // cluster status service
    clusterStatusSrv *ClusterStatusService
}

// TODO: test singleton feature (really per routine is a singleton???)

// return the only instance / singleton of a Broker
// (so every routine / thread would only have 1 Broker instance)
func GetBroker(configPath string) *Broker {
    syncLock.Do(func() {
        var err error
        brokerInstance, err = newBroker(configPath)
        if err != nil {
            // the only way to let the outside world know is "panic"
            panic(err)
        }
    })
    return brokerInstance
}

func newBroker(configPath string) (*Broker, error) {
    m := new(Broker)

    // config loading
    configInstancePtr, err := NewBrokerConfig(configPath)
    if err != nil {
        return nil, err
    }
    m.config = configInstancePtr

    // uuid
    _, uuid, err := m.getBrokerUUID()
    if err != nil {
        return nil, err
    }
    m.UUID = uuid

    // restful.Container, add modules to implement REST api(s) later on
    m.webserver = restful.NewContainer()

    // set logger
    m.GetFlexLogger()

    // set clusterStatus service
    m.clusterStatusSrv = NewClusterStatusService()

    return m, nil
}

// return the broker's UUID. 2 situations arise:
//
//  a) 1st time running the broker; create the UUID and store it inside "broker.id" file
//  b) "broker.id" file already existed, read back the UUID from the file instead
//
// return 3 information:
//  1) bool => is the broker started for the 1st time (true)
//  2) uuid => broker's uuid (could be generated or read from broker.id)
//  3) error => any error occurred
func (b *Broker) getBrokerUUID() (bool, string, error) {
    // check if broker.id.lock or broker.id file is there or not
    if queutil.IsFileExists(brokerIdFile) == true {
        uuidByteArr, err := queutil.ReadFileContent(brokerIdFile)
        if err != nil {
            return false, "", err
        }
        return false, string(uuidByteArr), nil

    } else {
        // assume no broker UUID (first time starting up the broker)
        uuid := queutil.GenerateUUID()
        err := queutil.WriteStringToFile(brokerIdFile, uuid)
        if err != nil {
            return false, "", err
        }
        return true, uuid, nil
    }
}

// method to start the broker instance
func (b *Broker) StartBroker() error {
    // TODO bootstrap logs
    l := b.logger
    l.Info([]byte("initializing...\n"))

    // a) start the routine to listen for exit signals
    go b.listenToExitSequences()
    l.Debug([]byte("exit sequence hooks added.\n"))

    // a2) load cluster status information
    b.clusterStatusSrv.LoadClusterStatus()

// TODO: add more modules (modules = rest API)
    // b) add api modules
    b.webserver.Add(NewStatsApiModule())
    l.Info([]byte("[modules - stats] added.\n"))
    b.webserver.Add(NewClusterStatusApiModule())
    l.Info([]byte("[modules - cluster status] added.\n"))

    // c) rename broker.id => broker.id.lock and LOCK the file as well
    err := queutil.RenameFile(brokerIdFile, brokerIdLockFile, 0444)
    if err != nil {
        return err
    }
    lockFilePath, err := b.appendFileToWD(brokerIdLockFile)
    if err != nil {
        return err
    }
    err = queutil.LockFile(lockFilePath)
    if err != nil {
        fmt.Printf("trying to lock [%v]\n", lockFilePath)
        return err
    }
    l.Debug([]byte("locking broker.id file -> broker.id.lock\n"))


    // d) start the http server
    return http.ListenAndServe(b.config.BrokerCommunicationAddress, b.webserver)
}

func (b *Broker) getPathSeparator() string {
    slash := "/"
    if strings.Compare(runtime.GOOS, "windows") == 0 {
        slash = "\\"
    }
    return slash
}

func (b *Broker) appendFileToWD(file string) (string, error) {
    slash := b.getPathSeparator()
    path, err := os.Getwd()
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%v%v%v", path, slash, file), nil
}

// resource(s) to be released before stopping the broker
func (b *Broker) Release(optionalParams map[string]interface{}) error {
    // resource release here
    l := b.logger
    l.Info([]byte("release resource(s) sequence [ACTIVATE]...\n"))

    err := queutil.RenameFile(brokerIdLockFile, brokerIdFile, 0444)
    if err != nil {
        return err
    }
    lockFilePath, err := b.appendFileToWD(brokerIdFile)
    if err != nil {
        return err
    }
    err = queutil.UnlockFile(lockFilePath)
    if err != nil {
        b.logger.Err([]byte(fmt.Sprintf("trying to unlock [%v] but got exception => [%v]\n", lockFilePath, err)))
        return err
    }
    l.Debug([]byte("released broker.id.lock -> broker.id\n"))

    // final updates on cluster status
    err = b.clusterStatusSrv.Release(nil)
    if err != nil {
        b.logger.Err([]byte(err.Error() + "\n"))
        return err
    }

    l.Info([]byte("release resource(s) sequence [DONE]\n"))
    return nil
}

// method to listen to lifecycle hooks based on signal-term or signal-interrupt
func (b *Broker) listenToExitSequences() {
    signalChannel := make(chan os.Signal, 1)
    signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

    sig := <- signalChannel
    fmt.Println("sig received =>", sig)
    // call cleanup method(s)
    err := b.Release(nil)
    if err != nil {
        b.logger.Err([]byte(err.Error() + "\n"))
        // fmt.Println(err)
    }
    os.Exit(1)
}


// add a web module / REST api to the underlying webservice object
func (b *Broker) AddModules(module *restful.WebService) error {
    if module == nil {
        return fmt.Errorf("invalid module supplied~ [%v]", module)
    }
    b.webserver.Add(module)

    return nil
}

// setup the FlexLogger for logging purpose
// (default is console and rolling-file logger)
func (b *Broker) GetFlexLogger() *queutil.FlexLogger {
    b.logger = queutil.NewFlexLogger()
    // append logger implementations
    b.logger.AddLogger(queutil.NewConsoleLogger()).AddLogger(queutil.NewRollingFileLogger(
        fmt.Sprint(b.config.LogFolder, b.getPathSeparator(), brokerLogFilename),
        b.config.LogMaxFileSizeInMb, b.config.LogRotationDays,
        b.config.LogRotationDays, false))

    // set level based on the config file
    switch b.config.LogLevel {
    case "debug":
        b.logger.LogLevel = queutil.LogLevelDebug
    case "info":
        b.logger.LogLevel = queutil.LogLevelInfo
    case "warn":
        b.logger.LogLevel = queutil.LogLevelWarn
    case "err":
        b.logger.LogLevel = queutil.LogLevelErr
    default:
        b.logger.LogLevel = queutil.LogLevelInfo
    }

    return b.logger
}
