package main

import (
    "github.com/emicklei/go-restful"
    "github.com/quoeamaster/queutil"
    "fmt"
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
    bArr, err := queutil.GetHttpRequestContent(req.Request)
    if err != nil {
        panic(err)
    }
    // deserialize... (could not use the json.unmarshal() directly...)
    fmt.Print(b, bArr)


}



// request a sync for cluster status with the Master broker in the cluster
func requestClusterStatusFromMaster (req *restful.Request, res *restful.Response) {

}

type ClusterSyncRequest struct {
    Seeds []BrokerSeed
}
