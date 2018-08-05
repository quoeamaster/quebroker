package main

import (
    "github.com/emicklei/go-restful"
    "fmt"
    "github.com/quoeamaster/queutil"
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
    bArr, err := queutil.GetHttpRequestContent(req.Request)
    if err != nil {
        panic(err)
    }
    fmt.Println (string(bArr))
    req.Request.Body.Close()
}


func startMasterElection (req *restful.Request, res *restful.Response) {
    fmt.Println(req.Request.Header)
}
