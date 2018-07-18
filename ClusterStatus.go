package main

import (
    "path/filepath"
    "os"
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
        { "name": "Item",
            "namespace": "que.broker.cluster",
            "type": "record", 
            "doc": "cluster status item(s) - simple key, value pairs",
            "fields": [
                { "name": "key", "type": "string" },
                { "name": "value", "type": "string" },
                { "name": "next", "type": [ "null", "Item" ] }
            ]
        }
    ]
}`

// a service object for manipulating ClusterStatus
type ClusterStatusService struct {

}

func NewClusterStatusService () *ClusterStatusService {
    m := new(ClusterStatusService)

    return m
}

func (s *ClusterStatusService) LoadClusterStates () {
    b := GetBroker("")

    // try to find the persisted states file (binary) under the "data" folder
    stateFilepath := filepath.Join(b.config.DataFolder, ".cluster_status")
    _, err := os.Stat(stateFilepath)
    if os.IsNotExist(err) {
        // assume 1st time start up; hence no cluster status for sure; write an empty file there

    } else {
        // load it into memory; sync with disk content on a regular basis

    }



}

