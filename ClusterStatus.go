package main

import (
    "path/filepath"
    "os"
    "gopkg.in/linkedin/goavro.v2"
    "queutil"
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
    clusterStatusMap map[string]interface{}

}

func NewClusterStatusService () *ClusterStatusService {
    var err error

    m := new(ClusterStatusService)

    // create the avro schema codec
    m.avroCodec, err = goavro.NewCodec(clusterStatusAvroSchema)
    if err != nil {
        panic(err)
    }

    m.clusterStatusMap = make(map[string]interface{})


    return m
}

func (s *ClusterStatusService) LoadClusterStatus () error {
    b := GetBroker("")

    // try to find the persisted states file (binary) under the "data" folder
    stateFilepath := filepath.Join(b.config.DataFolder, ".cluster_status")
    _, err := os.Stat(stateFilepath)
    if os.IsNotExist(err) {
        // assume 1st time start up; hence no cluster status for sure; write an empty file there
        err := s.initialClusterStatusWrite(stateFilepath)
        if err != nil {
            b.logger.Err([]byte(err.Error() + "\n"))
            return err
        }

    } else {
        // load it into memory; sync with disk content on a regular basis
        // TODO load from the .cluster_status file back to memory

    }
    return nil
}



func (s *ClusterStatusService) initialClusterStatusWrite (stateFilepath string) error {
    b := GetBroker("")

    s.clusterStatusMap["version"] = 1
    s.clusterStatusMap["cluster_name"] = b.config.ClusterName
    s.clusterStatusMap["status_list"] = make([]map[string]interface{}, 0)

    binaryArr, err := s.avroCodec.BinaryFromNative(nil, s.clusterStatusMap)
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