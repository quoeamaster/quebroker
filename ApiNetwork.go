package main

import (
    "github.com/emicklei/go-restful"
    "fmt"
    "github.com/quoeamaster/queutil"
    "encoding/json"
)

func NewNetworkApiModule () *restful.WebService {
    srv := new(restful.WebService)
    // only accepts json request(s) and generates json response(s)
    srv.Path("/_network").
        Consumes(restful.MIME_JSON).
        Produces(restful.MIME_JSON)
    // declare the REST methods with the http verbs

    srv.Route(srv.POST("/_handshake").To(handshake))
    srv.Route(srv.POST("/_startMasterElection").To(startMasterElection))

    return srv
}

// curl -XPOST localhost:10030/_network/_handshake -H 'Content-Type: application/json' -d '{"key1":"value1", "key2":"value2"}'

func handshake (req *restful.Request, res *restful.Response) {
    // try to get back any request parameter(s) sent
    bArr, err := queutil.GetHttpRequestContent(req.Request)
    if err != nil {
        panic(err)
    }
    request := new(NetworkHandshakeRequest)
    err = json.Unmarshal(bArr, request)
    if err != nil {
        panic(err)
    }
    fmt.Println ("$$ inside handshake api =>", request.ClusterName, request.SeedIP)

    // get back the Broker instance
    b := GetBroker("")
    fmt.Println ("$$ inside handshake api => broker.cluster:", b.clusterStatusSrv.memLevelClusterStatusMap)
    fmt.Println ("$$ inside handshake api => broker.brokername:", b.config.BrokerName)
    fmt.Println ("$$ inside handshake api => broker.isMaster:", b.isMaster)

    req.Request.Body.Close()
}


func startMasterElection (req *restful.Request, res *restful.Response) {
    fmt.Println(req.Request.Header)
}




type NetworkHandshakeRequest struct {
    ClusterName string
    SeedIP string
}
