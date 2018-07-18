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
    srv.Route(srv.GET("/config").To(getConfigurationStats))

    return srv
}

// return overall status of the broker instance (TODO: get real information, now it is for testing purpose)
func getOverallBrokerStats (req *restful.Request, res *restful.Response) {
    res.WriteHeaderAndJson(200, fmt.Sprintf("stats returned... %v", GetBroker("")), restful.MIME_JSON)
}

// return the configuration settings for this broker instance
func getConfigurationStats (req *restful.Request, res *restful.Response) {
    b := GetBroker("")

    // get the config(s) from config-file
    res.WriteHeader(200)
    res.Write([]byte(b.config.String()))
}


