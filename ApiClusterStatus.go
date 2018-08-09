package main

import (
    "github.com/emicklei/go-restful"
    "github.com/quoeamaster/queutil"
    "github.com/buger/jsonparser"
    "fmt"
    "encoding/json"
)

func NewClusterStatusApiModule () *restful.WebService {
    srv := new(restful.WebService)
    // only accepts json request(s) and generates json response(s)
    srv.Path("/_clusterstatus").
        Consumes(restful.MIME_JSON).
        Produces(restful.MIME_JSON)
    // declare the REST methods with the http verbs
    srv.Route(srv.GET("/").To(getOverallClusterStatus))
    srv.Route(srv.GET("/mastersync").To(requestClusterStatusFromMaster))

    srv.Route(srv.POST("/sync").To(syncClusterStatus))

    return srv
}

// return the overall cluster status.
// Combines both in-mem cluster status with persistable cluster status.
func getOverallClusterStatus (req *restful.Request, res *restful.Response) {

}

// a sync request on cluster status has been "fan" out to all broker(s)
// in the cluster. Usually this sync operation is set by the
// Master of the cluster.
func syncClusterStatus (req *restful.Request, res *restful.Response) {
    b := GetBroker("")
    memMap := make(map[string]interface{})
    perMap := make(map[string]interface{})
    bArr, err := queutil.GetHttpRequestContent(req.Request)
    if err != nil {
        panic(err)
    }
    // deserialize... and prepare the Map(s) for cluster status update
    syncClusterStatusOnBrokerSeeds(&memMap, &bArr)

    // PS. if the element is not available, the following block won't be invoked
    jsonparser.ArrayEach(bArr, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
        if value != nil {
            fmt.Println (string(value))
        } else {
            fmt.Println ("---> non exists for tag nonexists")
        }
    }, keyClusterStatusTypeMemory, "nonexists")

    // update cluster status
    // TODO: testing ... REMOVE later
    // err = b.clusterStatusSrv.MergeClusterStatus(memMap, perMap)
    err = b.clusterStatusSrv.MergeClusterStatus(memMap, perMap)
    if err != nil {
        b.logger.Err([]byte(fmt.Sprintf("[cluster_status] could not update cluster status; error => %v\n", err)))
        commonErr := NewCommonError(500, "", err)
        if err := res.WriteHeaderAndJson(500, commonErr, httpContentTypeJson); err != nil {
            panic(err)
        }

    } else {
        response := new(ClusterSyncResponse)
        for key, val := range memMap {
            if val != nil && !queutil.IsStringEmpty(key) {
                if response.MemClusterStatusUpdateKeys == nil {
                    response.MemClusterStatusUpdateKeys = make([]string, 0)
                }
                response.MemClusterStatusUpdateKeys = append(response.MemClusterStatusUpdateKeys, key)
            }
        }
        for key, val := range perMap {
            if val != nil && !queutil.IsStringEmpty(key) {
                if response.PersistentClusterStatusUpdateKeys == nil {
                    response.PersistentClusterStatusUpdateKeys = make([]string, 0)
                }
                response.PersistentClusterStatusUpdateKeys = append(response.PersistentClusterStatusUpdateKeys, key)
            }
        }
        res.WriteHeaderAndJson(200, *response, httpContentTypeJson)
    }
}

// method to handle the sync of Mem-cluster-status "Cluster seed list / members"
func syncClusterStatusOnBrokerSeeds (mapPtr *map[string]interface{}, bArr *[]byte) {
    seedList := make([]BrokerSeed, 0)
    jsonparser.ArrayEach(*bArr, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
        seed := new(BrokerSeed)
        err = json.Unmarshal(value, seed)
        if err != nil {
            panic (err)
        }
        seedList = append(seedList, *seed)
        // further extract value from the []byte
        // fmt.Println(jsonparser.Get(value, "url"))
    }, keyClusterStatusTypeMemory, keyClusterSeedList)
    if seedList != nil && len(seedList) > 0 {
        (*mapPtr)[keyClusterSeedList] = seedList
    }
    // GetBroker("").logger.Debug([]byte(fmt.Sprintf("[cluster_status] %v -> %v\n", len(seedList), seedList)))
}


// request a sync for cluster status with the Master broker in the cluster
func requestClusterStatusFromMaster (req *restful.Request, res *restful.Response) {

}

// Response representing a Cluster sync operation
type ClusterSyncResponse struct {
    MemClusterStatusUpdateKeys          []string
    PersistentClusterStatusUpdateKeys   []string
}