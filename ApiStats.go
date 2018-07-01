package main

import (
    "github.com/emicklei/go-restful"
    "fmt"
)

func NewStatsApiModule() *restful.WebService {
    srv := new(restful.WebService)
    // only accepts json request(s) and generates json response(s)
    srv.Path("/_stats").
        Consumes(restful.MIME_JSON).
        Produces(restful.MIME_JSON)
    // declare the REST methods with the http verbs
    srv.Route(srv.GET("/").To(getOverallBrokerStats))

    return srv
}

func getOverallBrokerStats(req *restful.Request, res *restful.Response) {
    res.WriteHeaderAndJson(200, fmt.Sprintf("stats returned... %v", GetBroker("")), restful.MIME_JSON)
}


