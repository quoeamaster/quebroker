package main

import (
    "path/filepath"
    "os"
    "gopkg.in/linkedin/goavro.v2"
    "fmt"
    "reflect"
    "strings"
    "bytes"
    "github.com/quoeamaster/queutil"
    "time"
)

// avro schema for cluster-status
const clusterStatusAvroSchema = `{
    "namespace": "que.broker.cluster",
    "name": "ClusterStatus",
    "type": "record",
    "doc": "avro schema for cluster-status (information shared between brokers in the cluster)",
    "fields": [
        { "name": "version", "type": "int" },
        { "name": "cluster_name", "type": "string" },
        { "name": "status_list",
            "namespace": "que.broker.cluster", 
            "doc": "cluster status item(s) - simple key, value pairs",
            "type": "array",
            "items": {
                "name": "status",
                "type": "record",
                "fields": [
                    { "name": "key", "type": "string" },
                    { "name": "value", "type": "string" }
                ]
            }
        }
    ]
}`

// struct to encapsulate the details of the current / active master broker
type ActiveMaster struct {
    BrokerId string
    BrokerName string
    BrokerCommunicationAddr string
    // since when did this active-master started to serve
    Since time.Time
}

// a service object for manipulating ClusterStatus
type ClusterStatusService struct {
    // TODO: some cluster status info is temporary (e.g. who is the master)
    // TODO: some are persistent-able

    // codec for the schema
    avroCodec *goavro.Codec

    // status in string value
    persistableClusterStatusMap map[string]interface{}

    // sort of temporary cluster status;
    // for example who is the master is a temp setting and would change
    // once the cluster is restarted or the current master broker is down.
    // usually DO NOT need to persist as these status changes
    memLevelClusterStatusMap map[string]interface{}

    // the path to locate for the local broker's .cluster_status file
    clusterStatusFilepath string
}

func NewClusterStatusService () *ClusterStatusService {
    var err error

    m := new(ClusterStatusService)

    // create the avro schema codec
    m.avroCodec, err = goavro.NewCodec(clusterStatusAvroSchema)
    if err != nil {
        panic(err)
    }

    m.persistableClusterStatusMap = make(map[string]interface{})
    m.memLevelClusterStatusMap = make(map[string]interface{})

    return m
}

// return the active-master broker information if any.
// There are cases when the cluster is still forming and hence no master broker yet.
// Also cluster might be formed however, the quorum number of master-ready
//  brokers are not available yet hence no master broker available
func (s *ClusterStatusService) GetActiveMaster () (activeMaster *ActiveMaster, err error) {
    err = nil
    activeMaster = nil
    isMasterAvailable := false
    // broker id
    if val := s.memLevelClusterStatusMap[keyClusterActiveMasterId]; val != nil {
        isMasterAvailable = true
        activeMaster = new(ActiveMaster)
        activeMaster.BrokerId = val.(string)
    }
    // if broker id exists, the other information should be there as well
    if isMasterAvailable {
        if val := s.memLevelClusterStatusMap[keyClusterActiveMasterName]; val != nil {
            activeMaster.BrokerName = val.(string)
        }
        if val := s.memLevelClusterStatusMap[keyClusterActiveMasterAddr]; val != nil {
            activeMaster.BrokerCommunicationAddr = val.(string)
        }
        if val := s.memLevelClusterStatusMap[keyClusterActiveMasterSince]; val != nil {
            activeMaster.Since = val.(time.Time)
        }
    }
    return activeMaster, err
}

// load back the persisted cluster status (if any)
func (s *ClusterStatusService) LoadClusterStatus () error {
    b := GetBroker("")

    // try to find the persisted states file (binary) under the "data" folder
    stateFilepath := s.getClusterStatusFilepath()
    _, err := os.Stat(stateFilepath)
    if os.IsNotExist(err) {
        // assume 1st time start up; hence no cluster status for sure; write an empty file there
        err := s.initialClusterStatusWrite()
        if err != nil {
            b.logger.Err([]byte(err.Error() + "\n"))
            return err
        }

    } else {
        // load it into memory; sync with disk content on a regular basis
        // load from the .cluster_status file back to memory
        _, err := s.loadClusterStatusFromFile()
        if err != nil {
            b.logger.Err([]byte(err.Error() + "\n"))
            return err
        }
        b.logger.Debug([]byte(fmt.Sprintf("loaded .cluster_status from local data repository => %v\n",
            s.memLevelClusterStatusMap)))
    }
    return nil
}

func (s *ClusterStatusService) getClusterStatusFilepath () string {
    if len(strings.TrimSpace(s.clusterStatusFilepath)) == 0 {
        b := GetBroker("")
        s.clusterStatusFilepath = filepath.Join(b.config.DataFolder, ".cluster_status")
    }
    return s.clusterStatusFilepath
}

// load the existed cluster status file back to memory
func (s *ClusterStatusService) loadClusterStatusFromFile () (map[string]interface{}, error) {
    stateFilepath := s.getClusterStatusFilepath()
    byteArr, err := queutil.ReadFileContent(stateFilepath)
    if err != nil {
        return nil, err
    }
    objectRead, _, err := s.avroCodec.NativeFromBinary(byteArr)
    if err != nil {
        return nil, err

    } else if objectRead == nil {
        return nil, err
    }
    // cast back to map[string]interface{}
    switch objectRead.(type) {
    case map[string]interface{}:
        memCSMap := reflect.ValueOf(objectRead).Interface().(map[string]interface{})
        s.memLevelClusterStatusMap = memCSMap

        return memCSMap, nil
    default:
        return nil, fmt.Errorf("corrupted file format for the cluster status")
    }
}


// when this broker is first started, persist the minimal
// cluster status (e.g. cluster_name)
func (s *ClusterStatusService) initialClusterStatusWrite () error {
    b := GetBroker("")
    stateFilepath := s.getClusterStatusFilepath()

    // extract the folder part
    fileIdx := strings.Index(stateFilepath, ".cluster_status")
    // create the folder hierarchy
    _, err := queutil.IsDirExists(stateFilepath[0:fileIdx], true)
    if err != nil {
        return err
    }

    s.persistableClusterStatusMap["version"] = 1
    s.persistableClusterStatusMap["cluster_name"] = b.config.ClusterName
    s.persistableClusterStatusMap["status_list"] = make([]map[string]interface{}, 0)

    binaryArr, err := s.avroCodec.BinaryFromNative(nil, s.persistableClusterStatusMap)
    if err != nil {
        return err
    }
    err = queutil.WriteByteArrayToFile(binaryArr, stateFilepath)
    if err != nil {
        return err
    }

    /* example on how to add statusList items
    // per status entry... a map?
    statusList := make([]map[string]interface{}, 0)
    statusMap := make(map[string]interface{})
    statusMap["key"] = "fake.master"
    statusMap["value"] = "FAKE.MASTER.TESTING"
    statusList = append(statusList, statusMap)

    statusMap = make(map[string]interface{})
    statusMap["key"] = "fake.iteration"
    statusMap["value"] = fmt.Sprintf("%v", 999.89)
    statusList = append(statusList, statusMap)

    mapValues["status_list"] = statusList


    // binary version of the map
    binaryArr, err := s.avroCodec.BinaryFromNative(nil, mapValues)
    if err != nil {
        return err
    }
    err = queutil.WriteByteArrayToFile(binaryArr, stateFilepath)
    if err != nil {
        return err
    }

    // load from the cluster file
    binaryArr, err = queutil.ReadFileContent(stateFilepath)
    if err != nil {
        return err
    }
    obj, _, err := s.avroCodec.NativeFromBinary(binaryArr)
    mapValues = reflect.ValueOf(obj).Interface().(map[string]interface{})
    fmt.Println(mapValues)
    */

    return nil
}

func (s *ClusterStatusService) mergeValues (broker *Broker, existValue interface{}, targetValue interface{}) (finalValue interface{}) {
    // assume only unique value would be maintained
    if targetValue != nil && reflect.ValueOf(existValue) == reflect.ValueOf(targetValue) {
        broker.logger.Info([]byte(fmt.Sprintf("[cluster_status] inside mergeValues => same type of existing and targetValue, type => [%v]\n", reflect.TypeOf(targetValue))))
        // array or map?
        existsType := reflect.TypeOf(existValue)
        if existsType.Kind() == reflect.Slice {
            broker.logger.Info([]byte ("[cluster_status] slice is found\n"))

            // brittle way...
            switch existValue.(type) {
            case []BrokerSeed:
                existSlice, _   := existValue.([]BrokerSeed)
                targetSlice, _  := targetValue.([]BrokerSeed)
                // TODO metric calculation.. to be removed
                finalSlice := make([]BrokerSeed, 0)
                iLoops := 0
                iExpectedLoops := len(existSlice) + len(targetSlice)
                // append only the missing ones
                for _, tVal := range targetSlice {
                    for _, eVal := range existSlice {
                        iLoops+=1
                        if strings.Compare(eVal.String(), tVal.String()) == 0 {
                            break
                        }
                    }
                    finalSlice = append(finalSlice, tVal)
                }
                broker.logger.Info([]byte (fmt.Sprintf("[cluster_status] merged => %v\n", finalSlice)))
                broker.logger.Debug([]byte (fmt.Sprintf("[cluster_status] iLoop vs iExpectedLoops => %v vs %v\n", iLoops, iExpectedLoops)))

                return finalSlice
            // TODO: add other types handling here
            default:
                broker.logger.Warn([]byte(fmt.Sprintf("[cluster_status] unsupported type [%v]\n", existsType)))
            }

        } else if existsType.Kind() == reflect.Map {
            broker.logger.Info([]byte ("[cluster_status] map is found\n"))
// TODO: tbd on map operation(s)
        } else {
            broker.logger.Info([]byte (fmt.Sprintf("[cluster_status] %v is found\n", existsType.Kind())))
        }
    }
    return existValue
}

// merge changes in the cluster status (both in-mem and persistable status)
func (s *ClusterStatusService) MergeClusterStatus (
    memClusterStatusMap map[string]interface{},
    persistableClusterStatusMap map[string]interface{}) error {

    var buf bytes.Buffer
    numMerges := 0
    b := GetBroker("")

    buf.WriteString("[cluster_srv] cluster status (in-memory) merged by the following keys: ")

    if memClusterStatusMap != nil {
        for key, value := range memClusterStatusMap {
            if s.memLevelClusterStatusMap[key] != nil {
                buf.WriteString(key)
                numMerges += 1
                // merge changes if necessary (e.g. if the original value is a Map or Array)
                finalValue := s.mergeValues(b, s.memLevelClusterStatusMap[key], value)
                s.memLevelClusterStatusMap[key] = finalValue

            } else {
                s.memLevelClusterStatusMap[key] = value
            }
        }
    }
    if numMerges == 0 {
        buf.WriteString("none")
    }
    buf.WriteString(". cluster status (persistent) merged by the following keys: ")
    numMerges = 0

    if persistableClusterStatusMap != nil {
        for key, value := range persistableClusterStatusMap {
            if s.persistableClusterStatusMap[key] != nil {
                buf.WriteString(key)
                numMerges += 1
                // merge changes if necessary (e.g. if the original value is a Map or Array)
                finalValue := s.mergeValues(b, s.persistableClusterStatusMap[key], value)
                s.persistableClusterStatusMap[key] = finalValue

            } else {
                s.persistableClusterStatusMap[key] = value
            }
        }
    }
    if numMerges == 0 {
        buf.WriteString("none")
    }
    buf.WriteString(".\n")
    // log [info]
// TODO: to debug later on
    b.logger.Info(buf.Bytes())

    // debug
    b.logger.Debug([]byte(fmt.Sprintf("[cluster_srv] %v\n", s.memLevelClusterStatusMap)))
    b.logger.Debug([]byte(fmt.Sprintf("[cluster_srv] %v\n", s.persistableClusterStatusMap)))

    return nil
}

// persist the current persist-able cluster status to disk
func (s *ClusterStatusService) PersistClusterStatusToFile () error {
    // only persist the persistable cluster status to file
    stateFilepath := s.getClusterStatusFilepath()

    byteArr, err := s.avroCodec.BinaryFromNative(nil, s.persistableClusterStatusMap)
    if err != nil {
        return err
    }
    err = queutil.WriteByteArrayToFile(byteArr, stateFilepath)
    if err != nil {
        return err
    }
    // log [debug]
    GetBroker("").logger.Debug([]byte(".cluster_status persisted\n"))

    return nil
}

// implementation of IReleasable interface
func (s *ClusterStatusService) Release(optionalParam map[string]interface{}) error {
    // TODO: ask for final cluster_status sync???
    // if I am the master => sync out to the rest of the broker(s) within the cluster
    // if I am NOT the master => ask for the master's latest cluster status and try to sync before exit

    // TODO: now... merge the changes and write to disk

    return nil
}

// return the value under the given key (to the caller, it doesn't matter
// whether the cluster status is MEMory or PERsistent level
func (s *ClusterStatusService) GetClusterStatusByKey (key string) interface{} {
    broker := GetBroker("")

    // mem level
    // PS. for mem level .. they could be normal golang objects;
    // whilst for persistent level, they might be json formatted...
    // (conversion back to golang object might be required)
    val := s.memLevelClusterStatusMap[key]
    if val == nil {
        // try persistent level
        val = s.persistableClusterStatusMap[key]
    }

    switch val.(type) {
    case string:
        // do nothing...
        broker.logger.Info([]byte(fmt.Sprintf("[cluster_srv] cluster status under key [%v] is string [%v]\n", key, val.(string))))

    case []BrokerSeed:
        // convert anything else back to string (json)
        seedList := val.([]BrokerSeed)
        finalSeedList := make([]interface{}, 0)
        for i := range seedList {
            seed := seedList[i]
            finalSeedList = append(finalSeedList, &seed)
        }
        // json-fy
        var b bytes.Buffer
        b = queutil.BeginJsonStructure(b)
        b = queutil.AddArrayToJsonStructure(b, keyClusterSeedList, finalSeedList)
        b = queutil.EndJsonStructure(b)
        val = b.String()

    default:
        broker.logger.InfoString(fmt.Sprintf("==> no idea, type is %v\n", reflect.TypeOf(val)))
    }

    return val
}


// struct for cluster mem level reference
type BrokerSeed struct {
    UUID string         `json:"BrokerId"`
    Name string         `json:"BrokerName"`
    Addr string         `json:"BrokerCommunicationAddr"`
    RoleMaster bool     `json:"RoleMaster"`
    RoleData bool       `json:"RoleData"`
    Timestamp int64     `json:"Since"`
}
func (b *BrokerSeed) String () string {
    var buf bytes.Buffer

    buf = queutil.BeginJsonStructure(buf)
    buf = queutil.AddStringToJsonStructure(buf, "BrokerId", b.UUID)
    buf = queutil.AddStringToJsonStructure(buf, "BrokerName", b.Name)
    buf = queutil.AddStringToJsonStructure(buf, "BrokerCommunicationAddr", b.Addr)
    buf = queutil.AddBoolToJsonStructure(buf, "RoleMaster", b.RoleMaster)
    buf = queutil.AddBoolToJsonStructure(buf, "RoleData", b.RoleData)
    buf = queutil.AddInt64ToJsonStructure(buf, "Since", b.Timestamp)

    buf = queutil.EndJsonStructure(buf)

    return buf.String()
}
func NewBrokerSeed(uuid, name, addr string, master, data bool) BrokerSeed {
    b := new(BrokerSeed)

    b.UUID = uuid
    b.Name = name
    b.Addr = addr
    b.RoleMaster = master
    b.RoleData = data

    return *b
}


