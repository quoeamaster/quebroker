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
    "bytes"
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

    asyncApiChannel chan string

    // the http.client for this broker instance (re-use)
    restClient *http.Client
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
    m.asyncApiChannel = make(chan string, 1)

    timeout, err := time.ParseDuration("30s")
    if err != nil {
        // can't help... sometime really wrong on the given parse duration string
        panic(err)
    }
    m.restClient = queutil.GenerateHttpClient(timeout, nil, nil, nil)

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

    go b.listenToAsyncApiReturn()

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

// ------------------------------------------------------------------------------------------------------------

func (b *Broker) createOrJoinCluster () {
    b.logger.Debug([]byte("** inside createOrJoinCluster routine"))

    iSignal := <- b.clusterJoinChannel
    switch iSignal {
    case ChanSignalCreateOrJoinCluster:
        // force to sleep for 1000 ms
        // (sort of throttle to keep the broker less busy during startup) // todo: could be a RANDOM interval instead
        time.Sleep(time.Millisecond * 1000)

        brokerSeedVOPtr := new(queutil.BrokerSeedVO)
        brokerSeedVOPtr.ClusterName = b.config.ClusterName
        brokerSeedVOPtr.DiscoveryBrokerSeeds = b.config.ClusterDiscoverySimpleSeeds
        brokerSeedVOPtr.BrokerCommunicationAddr = b.config.BrokerCommunicationAddress

        if queutil.IsStringEmpty(b.config.SecurityScheme) {
            brokerSeedVOPtr.SecurityScheme = ""
        } else {
            brokerSeedVOPtr.SecurityScheme = b.config.SecurityScheme
        }
        optionMap := make(map[string]interface{})
        optionMap[KeyDiscoveryLogger] = b.logger

        valid, jsonResponse, _, err := b.discoveryPlugin.Ping(brokerSeedVOPtr.JsonString(), optionMap)

        /*
        optionMap[KeyDiscoveryClusterName]  = b.config.ClusterName
        optionMap[KeyDiscoverySeedList]     = b.config.ClusterDiscoverySimpleSeeds

        if !queutil.IsStringEmpty(b.config.SecurityScheme) {
            optionMap[KeyDiscoverySecurityScheme] = b.config.SecurityScheme
        } else {
            optionMap[KeyDiscoverySecurityScheme] = ""
        }

        valid, returnMap, err := b.discoveryPlugin.Ping(b.config.BrokerCommunicationAddress, optionMap)
        */


        // valid to join cluster with the target seedList (1 of them)
        if valid {
            // bArr := returnMap[KeyDiscoveryHandshakeResponseByteArray].([]byte)
            /*  message format in json should be the following
                {
                 "CanJoin": true,
                 "IsMasterReady": true,
                 "IsDataReady": true,
                 "IsActiveMaster": false,
                 "BrokerName": "broker_001",
                 "BrokerCommunicationAddr": "localhost:10030",
                 "BrokerId": "-LJNz7_bBnf8fcBlawCV"
                }
             */
            brokerSeedVOResponsePtr := new(queutil.BrokerSeedVO)
            //err = json.Unmarshal(bArr, brokerSeedVOResponsePtr)
            err = json.Unmarshal([]byte(jsonResponse), brokerSeedVOResponsePtr)
            if err != nil {
                b.logger.WarnString(fmt.Sprintf("[broker] failed to unmarshal data from the response of [%v], error => [%v]\n", brokerSeedVOResponsePtr.BrokerCommunicationAddr, err.Error()))
            }
            // can join.. hooray -> broadcast to the members (actively known online)
            // for cluster_status update (broker list)
            // sniffing is done after master election (not that important in general)
            if brokerSeedVOResponsePtr.CanJoin {
                // update itself and target seed's "cluster seed list"
                brokerSeedVOList, seedListString, err := b.updateClusterSeeds (brokerSeedVOResponsePtr)
b.logger.InfoString("@@@ well... tbd\n\n")
b.logger.InfoString(fmt.Sprintf("%v\n", brokerSeedVOList))
b.logger.InfoString(fmt.Sprintf("%v\n", seedListString))
b.logger.InfoString(fmt.Sprintf("%v\n", err))

                // NEXT... election (TODO: add rules on quorum here???)
                optionMap = make(map[string]interface{})
                // json-fy version of the cluster seeds
                // ** optionMap[KeyDiscoverySeedList] = b.clusterStatusSrv.GetClusterStatusByKey(keyClusterSeedList)
                optionMap[KeyDiscoveryLogger] = b.logger
                b.logger.InfoString(fmt.Sprintf("[broker] %v\n", b.clusterStatusSrv.GetClusterStatusByKey(keyClusterSeedList)))

                /*
                masterId, masterMap, err := b.discoveryPlugin.ElectMaster(optionMap)
                // something wrong with master election consider it as hazardous
                if err != nil || queutil.IsStringEmpty(masterId) {
                    panic (queutil.CreateErrorWithString("could not elect a master! Due to some reasons, please check the configuration files if there were any master-ready brokers"))
                }
                b.logger.InfoString(fmt.Sprintf("*** %v %v\n", masterId, masterMap))
                // async way to broadcast the active master's info to the cluster members
                b.broadcastActiveMasterToSeeds(brokerSeedVOList, masterMap)
*/




            }

        } else {
            b.logger.Info([]byte(fmt.Sprintf("[broker] no broker(s) in the seed list able to form a cluster; hence starting up the cluster [%v] by itself\n", b.config.ClusterName)))
            // TODO: startup on its own
        }

    default:
        // TODO: should it panic???
        b.logger.Warn([]byte(fmt.Sprintf("unknown signal received on the 'clusterJoinChannel' => %v\n", iSignal)))
    }
    // close the createOrJoin channel (later on should use any routine or channel to re-elect new master after active master is down??)
    close(b.clusterJoinChannel)
}

// update itself's cluster status, also the target seed's cluster status
// on the "cluster seed list" (ping-able seed list)
func (b *Broker) updateClusterSeeds (brokerSeedVOResponsePtr *queutil.BrokerSeedVO) ([]queutil.BrokerSeedVO, string, error) {
    brokersList := make([]queutil.BrokerSeedVO, 0)
    // itself
    brokerSeedVOPtr := new(queutil.BrokerSeedVO)
    brokerSeedVOPtr.BrokerId = b.UUID
    brokerSeedVOPtr.BrokerName = b.config.BrokerName
    brokerSeedVOPtr.BrokerCommunicationAddr = b.config.BrokerCommunicationAddress
    brokerSeedVOPtr.IsMasterReady = b.config.RoleMasterReady
    brokerSeedVOPtr.IsDataReady = b.config.RoleDataReady

    brokersList = append(brokersList, *brokerSeedVOPtr)
    // seed
    if strings.Compare(b.UUID, brokerSeedVOResponsePtr.BrokerId) != 0 {
        brokersList = append(brokersList, *brokerSeedVOResponsePtr)
    }

    // update the cluster_status of itself first
    memMap := make(map[string]interface{})
    memMap[keyClusterSeedList] = brokersList
    err := b.clusterStatusSrv.MergeClusterStatus(memMap, nil)
    if err != nil {
        b.logger.ErrString(fmt.Sprintf("[broker] failed to update *LOCAL* cluster_status, reason: %v\n", err))
    }

    // update the cluster status of the target seed as well
    protocol := b.config.SecurityScheme
    if len(strings.TrimSpace(protocol)) == 0 {
        protocol = ""
    }
    seedListString, err := b.syncTargetBrokerClusterStatus(brokersList, protocol)
    if err != nil {
        b.logger.Err([]byte(fmt.Sprintf("[broker] failed to update cluster_status for *SEEDS*, reason: %v\n", err)))
    }
    b.logger.InfoString(fmt.Sprintf("[broker] cluster [%v] formed\n", b.config.ClusterName))

    return brokersList, seedListString, nil
}

func (b *Broker) broadcastActiveMasterToSeeds (seeds []BrokerSeed, masterMap map[string]interface{}) {
    if seeds != nil && len(seeds) > 0 {
        // stringify the active master's data
        var contentBody bytes.Buffer
        masterBroker := *new(BrokerSeed)
        masterBroker.UUID = masterMap["brokerId"].(string)
        masterBroker.Name = masterMap["brokerName"].(string)
        masterBroker.Addr = masterMap["brokerCommAddr"].(string)
        masterBroker.Timestamp = (masterMap["brokerSince"].(time.Time)).Unix()
        contentBody = queutil.ConvertInterfaceToJsonStructure(contentBody, masterBroker)
        // TODO: wrap the above content under the structure of
        /*{
            "keyClusterStatusTypeMemory": {
                "keyClusterSeedList": [
                    {"BrokerId": "-LJNz7_bBnf8fcBlawCV","BrokerName": "broker_001","BrokerCommunicationAddr": "localhost:10030","RoleMaster": true,"RoleData": true,"Since": 0},
                    {"BrokerId": "-LJXsdIbCqLcX9cfW1UI","BrokerName": "broker_002","BrokerCommunicationAddr": "localhost:10031","RoleMaster": true,"RoleData": false,"Since": 0}
                ]
            }
            PS. add a "apiId" => _network/_handshake OR _network/_startMasterElection
        }*/

        for _, seed := range seeds {
            // create url
            clusterStatusSyncUrl := queutil.BuildGenericApiUrl(seed.Addr, "", "_clusterstatus/sync")
            go b.asyncApiCall(clusterStatusSyncUrl, contentBody, httpMethodPost)
        }
    }

    // asyncApiChannel
}

func (b *Broker) listenToAsyncApiReturn() {
    // should be non nil and json formatted
    returnContent := <- b.asyncApiChannel

    b.logger.InfoString(fmt.Sprintf("[broker] async api return => %v\n", returnContent))
}

func (b *Broker) asyncApiCall (apiUrl string, contentBody bytes.Buffer, httpMethod string) {
    switch httpMethod {
    case httpMethodGet:
        b.logger.InfoString("*** not yet implemented ***")

    case httpMethodPost:
        res, err := b.restClient.Post(apiUrl, httpContentTypeJson, &contentBody)
        if err != nil {
            b.logger.WarnString(fmt.Sprintf("[broker] calling \"%v\" got error => %v\n", apiUrl, err))
        } else {
            bArr, err := queutil.GetHttpResponseContent(res)
            if err != nil {
                b.logger.WarnString(fmt.Sprintf("[broker] calling \"%v\" OK but reading the response content got error => %v\n", apiUrl, err))
            } else {
                b.asyncApiChannel <- string(bArr)
            }
        }
    default:
        b.logger.WarnString(fmt.Sprintf("[broker] non supported http method [%v]\n", httpMethod))
    }
}

// update the seed list with the target broker member
func (b *Broker) syncTargetBrokerClusterStatus(brokerSeeds []queutil.BrokerSeedVO, protocol string) (string, error) {

    if brokerSeeds != nil && len(brokerSeeds) > 0 {
        // last seed member is the target
        seed := brokerSeeds[len(brokerSeeds)-1]
        syncClusterStatusUrl := queutil.BuildGenericApiUrl(
            seed.BrokerCommunicationAddr, protocol, "_clusterstatus/sync")

        finalSeedList := make([]interface{}, 0)
        for idx := range brokerSeeds {
            seed := brokerSeeds[idx]
            finalSeedList = append(finalSeedList, &seed)
        }
        /*
         *  sample json to be created =>
         * {"keyClusterStatusTypeMemory": {"keyClusterSeedList": [
         *  {"BrokerId": "-LJNyr2mOuIDOiEUStzp","BrokerName": "broker_002","BrokerCommunicationAddr": "localhost:10031","RoleMaster": true,"RoleData": false},
         *  {"BrokerId": "-LJNz7_bBnf8fcBlawCV","BrokerName": "broker_001","BrokerCommunicationAddr": "localhost:10030","RoleMaster": true,"RoleData": true}
         * ]}}
         */
        var buf bytes.Buffer

        buf = queutil.BeginJsonStructure(buf)
        buf = queutil.BeginObjectJsonStructure(buf, keyClusterStatusTypeMemory)
        buf = queutil.AddArrayToJsonStructure(buf, "keyClusterSeedList", finalSeedList)
        buf = queutil.EndObjectJsonStructure(buf)
        buf = queutil.EndJsonStructure(buf)
        seedListJson := buf.String()
        b.logger.Debug([]byte(fmt.Sprintf("[broker] serialized broker seeds => %v\n", buf.String())))

        res, err := b.restClient.Post(syncClusterStatusUrl, httpContentTypeJson, &buf)
        if err != nil {
            return "", err
        }

        // translate the return message
        if res.StatusCode != 200 {
            bArr, err := queutil.GetHttpResponseContent(res)
            if err != nil {
                return "", err
            }
            // b.logger.Info([]byte(fmt.Sprintf("[broker] response from cluster status sync => [%v]\n", string(bArr))))
            return "", queutil.CreateErrorWithString(fmt.Sprintf("something is wrong when updating cluster-status; response => %v", string(bArr)))
        }
        return seedListJson, nil

    } else {
        b.logger.Warn([]byte("[broker] no broker seed list provided HENCE no cluster operation done"))
        return "", nil
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
