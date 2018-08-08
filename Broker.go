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
    "github.com/quoeamaster/queutil"
    "github.com/emicklei/go-restful"
    "os"
    "os/signal"
    "syscall"
    "fmt"
    "sync"
    "strings"
    "runtime"
    "path"
    "github.com/quoeamaster/queplugin"
    "net/http"
    "time"
    "encoding/json"
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

    // is this Broker instance the MASTER (can update cluster status)
    isMaster bool

    // the implementation of the discovery plugin
    discoveryPlugin queplugin.DiscoveryPlugin

    // channel for clusterJoin related activities (int is used to save network overhead)
    clusterJoinChannel chan int
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

    // wait till an election has been done; then this broker instance could
    // become a Master or not
    m.isMaster = false

    // init the channel ready for cluster join / create activities
    m.clusterJoinChannel = make(chan int, 1)

    return m, nil
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
    b.webserver.Add(NewNetworkApiModule())
    l.Info([]byte("[modules - network status] added.\n"))
    b.webserver.Add(NewStatsApiModule())
    l.Info([]byte("[modules - stats] added.\n"))
    b.webserver.Add(NewClusterStatusApiModule())
    l.Info([]byte("[modules - cluster status] added.\n"))

    // c) rename broker.id => broker.id.lock and LOCK the file as well
    finalBrokerIdFile := path.Join(b.config.DataFolder, brokerIdFile)
    finalBrokerIdLockFile := path.Join(b.config.DataFolder, brokerIdLockFile)
    err := queutil.RenameFile(finalBrokerIdFile, finalBrokerIdLockFile, 0444)
    if err != nil {
        return err
    }
    err = queutil.LockFile(finalBrokerIdLockFile)
    if err != nil {
        fmt.Printf("trying to lock [%v]\n", finalBrokerIdLockFile)
        return err
    }
    l.Debug([]byte("locking broker.id file -> broker.id.lock\n"))

    // create the correct DiscoveryPlugin instance then ...
    b.discoveryPlugin = b.createDiscoveryModuleByModuleName(b.config.DiscoveryModuleName)
    go b.createOrJoinCluster()
    b.clusterJoinChannel <- ChanSignalCreateOrJoinCluster

    // sniff for valid seed list members, then trigger Master election


    // d) start the http server
    return http.ListenAndServe(b.config.BrokerCommunicationAddress, b.webserver)
}

// method to create the correct DiscoveryPlugin implementation based on
// moduleName. This method would panic if the provided moduleName is not
// recognized.
func (b *Broker) createDiscoveryModuleByModuleName (moduleName string) queplugin.DiscoveryPlugin {
    switch moduleName {
    case "simple":
        return NewSimpleDiscoveryPlugin()
    default:
        panic(fmt.Errorf("could not create the DiscoveryPlugin as type [%v] is unknown", moduleName))
    }
}

func (b *Broker) createOrJoinCluster () {
    b.logger.Debug([]byte("** inside createOrJoinCluster routine"))

    iSignal := <- b.clusterJoinChannel
    switch iSignal {
    case ChanSignalCreateOrJoinCluster:
        // force to sleep for 1000 ms
        // (sort of throttle to keep the broker less busy during startup)
        time.Sleep(time.Millisecond * 1000)

        optionMap := make(map[string]interface{})
        optionMap[KeyDiscoveryClusterName]  = b.config.ClusterName
        optionMap[KeyDiscoverySeedList]     = b.config.ClusterDiscoverySimpleSeeds
        optionMap[KeyDiscoveryLogger]       = b.logger

        valid, returnMap, err := b.discoveryPlugin.Ping(b.config.BrokerCommunicationAddress, optionMap)
        // valid to join cluster with the target seedList (1 of them)
        if valid {
            bArr := returnMap[KeyDiscoveryHandshakeResponseByteArray].([]byte)

            networkHandshakeResponse := new(NetworkHandshakeResponse)
            err = json.Unmarshal(bArr, networkHandshakeResponse)
            if err != nil {
                b.logger.Warn([]byte(fmt.Sprintf("[broker] failed to unmarshal data from the response of [%v], error => [%v]\n", networkHandshakeResponse.BrokerCommunicationAddr, err.Error())))
            }
            // can join.. hooray -> broadcast to the members (actively known online)
            // for cluster_status update (broker list)
            // sniffing is done after master election (not that important in general)
            if networkHandshakeResponse.CanJoin {
                brokersList := make([]BrokerSeed, 0)
                // itself
                brokersList = append(brokersList, NewBrokerSeed(
                    b.UUID, b.config.BrokerName,
                    b.config.BrokerCommunicationAddress,
                    b.config.RoleMasterReady, b.config.RoleDataReady))
                // seed
                brokersList = append(brokersList, NewBrokerSeed(
                    networkHandshakeResponse.BrokerId,
                    networkHandshakeResponse.BrokerName,
                    networkHandshakeResponse.BrokerCommunicationAddr,
                    networkHandshakeResponse.IsMasterReady,
                    networkHandshakeResponse.IsDataReady))

                memMap := make(map[string]interface{})
                memMap[keyClusterSeedList] = brokersList

                err := b.clusterStatusSrv.MergeClusterStatus(memMap, nil)
                if err != nil {
                    // TODO: panic or just log?
                    b.logger.Err([]byte(fmt.Sprintf("[broker] failed to update cluster_status, reason: %v\n", memMap)))
                }
                // send update to the target seed as well

                // TODO NEXT... election
            }

        } else {
            b.logger.Info([]byte(fmt.Sprintf("[broker] no broker(s) in the seed list able to form a cluster; hence starting up the cluster [%v] by itself\n", b.config.ClusterName)))
            // TODO: startup on its own
        }

/*
                if networkHandshakeResponse.CanJoin {
                    canBreakRetry = true

// broadcast => pending_master_election : true ; member list (itself plus the active responding broker)
// return value => success or not (sync status) plus the pending_master_election_timestamp (start of election request)
//  the smaller request time wins and determines which side should run the election
                }
 */
    default:
        // TODO: should it panic???
        b.logger.Warn([]byte(fmt.Sprintf("unknown signal received on the 'clusterJoinChannel' => %v\n", iSignal)))
    }
}

// TODO: sniffing should be extracted out a plugin(s) since different OS or
// TODO: hosting environment would have different ways to sniff members + plus Master election

func (b *Broker) sniffSeedMembers () {

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
    // TODO: broker.id should be located under the "data" folder
    brokerIdPath := path.Join(b.config.DataFolder, brokerIdFile)

    if queutil.IsFileExists(brokerIdPath) == true {
        uuidByteArr, err := queutil.ReadFileContent(brokerIdPath)
        if err != nil {
            return false, "", err
        }
        return false, string(uuidByteArr), nil

    } else {
        // assume no broker UUID (first time starting up the broker)
        uuid := queutil.GenerateUUID()
        err := queutil.WriteStringToFile(brokerIdPath, uuid)
        if err != nil {
            return false, "", err
        }
        return true, uuid, nil
    }
}

func (b *Broker) getPathSeparator() string {
    // PS. in actual, if using path.Join api; then the correct path separator
    // would be appended automatically; hence this method might be obsolete
    // in the future
    slash := "/"
    if strings.Compare(runtime.GOOS, "windows") == 0 {
        slash = "\\"
    }
    return slash
}

func (b *Broker) appendFileToWD(file string) (string, error) {
    workDir, err := os.Getwd()
    if err != nil {
        return "", err
    }
    return path.Join(workDir, file), nil
    // slash := b.getPathSeparator()
    // return fmt.Sprintf("%v%v%v", workDir, slash, file), nil
}

// resource(s) to be released before stopping the broker
func (b *Broker) Release(optionalParams map[string]interface{}) error {
    // resource release here
    l := b.logger
    l.Info([]byte("release resource(s) sequence [ACTIVATE]...\n"))

    finalBrokerIdLockFile := path.Join(b.config.DataFolder, brokerIdLockFile)
    finalBrokerIdFile := path.Join(b.config.DataFolder, brokerIdFile)
    err := queutil.RenameFile(finalBrokerIdLockFile, finalBrokerIdFile, 0444)
    if err != nil {
        return err
    }
    err = queutil.UnlockFile(finalBrokerIdFile)
    if err != nil {
        b.logger.Err([]byte(fmt.Sprintf("trying to unlock [%v] but got exception => [%v]\n", finalBrokerIdFile, err)))
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
    // however "kill -9 pid" is not caught-able by golang... weird...
    signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

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
