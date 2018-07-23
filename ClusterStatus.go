package main

import (
    "path/filepath"
    "os"
    "gopkg.in/linkedin/goavro.v2"
    "queutil"
    "fmt"
    "reflect"
    "strings"
    "bytes"
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

// merge changes in the cluster status (both in-mem and persistable status)
func (s *ClusterStatusService) MergeClusterStatus (
    memClusterStatusMap map[string]interface{},
    persistableClusterStatusMap map[string]interface{}) error {

    var buf bytes.Buffer
    numMerges := 0

    buf.WriteString("cluster status (in-memory) merged by the following keys: ")

    if memClusterStatusMap != nil {
        for key, value := range memClusterStatusMap {
            if s.memLevelClusterStatusMap[key] != nil {
                buf.WriteString(key)
                numMerges += 1
            }
            s.memLevelClusterStatusMap[key] = value
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
            }
            s.persistableClusterStatusMap[key] = value
        }
    }
    if numMerges == 0 {
        buf.WriteString("none")
    }
    buf.WriteString(".\n")
    // log [info]
    GetBroker("").logger.Info(buf.Bytes())

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