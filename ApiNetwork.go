package main

import (
    "github.com/emicklei/go-restful"
    "fmt"
    "github.com/quoeamaster/queutil"
    "encoding/json"
    "strings"
    "bytes"
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
    // get back the Broker instance
    b := GetBroker("")
    if b.logger != nil {
        b.logger.Debug([]byte(fmt.Sprintf ("[network] inside handshake api => %v, %v\n", request.ClusterName, request.SeedIP)))
    }

    response := new(NetworkHandshakeResponse)
    if strings.Compare(b.config.ClusterName, request.ClusterName) == 0 {
        response.CanJoin = true
    } else {
        response.CanJoin = false
    }
// TODO: update the role(s) when necessary in the future (for now only Master.Ready and Data.Ready role)
    response.IsMasterReady = b.config.RoleMasterReady
    response.IsDataReady = b.config.RoleDataReady

    response.IsActiveMaster = b.isMaster

    response.BrokerName = b.config.BrokerName
    response.BrokerCommunicationAddr = b.config.BrokerCommunicationAddress
    response.BrokerId = b.UUID

    // close request body as already read all parameters
    req.Request.Body.Close()

    // write out to the response
    // bArr, err = json.Marshal(response)
    if response.CanJoin {
        err = res.WriteHeaderAndJson(200, response, restful.MIME_JSON)
    } else {
        // accepted (sort of ok but not the perfect situation;
        // in this case everything fine except can't join the cluster)
        err = res.WriteHeaderAndJson(202, response, restful.MIME_JSON)
    }
    if err != nil {
        panic(err)
    }
}


func startMasterElection (req *restful.Request, res *restful.Response) {
    fmt.Println(req.Request.Header)
}



// Request => Handshake api (Network Module)
type NetworkHandshakeRequest struct {
    ClusterName string
    SeedIP string
}

// Response => Handshake api (Network Module)
type NetworkHandshakeResponse struct {
    // ok to join ? (same cluster_name or not)
    CanJoin bool
    // roles
    IsMasterReady bool
    IsDataReady bool
    // is this broker a Master already?
    IsActiveMaster bool
    // broker name
    BrokerName string
    BrokerCommunicationAddr string
    BrokerId string
}
func (n *NetworkHandshakeResponse) String () string {
    var buf bytes.Buffer

    buf.WriteString("canJoin: ")
    buf.WriteString(fmt.Sprintf("%v", n.CanJoin))
    buf.WriteString(", isMasterReady: ")
    buf.WriteString(fmt.Sprintf("%v", n.IsMasterReady))
    buf.WriteString(", isDataReady: ")
    buf.WriteString(fmt.Sprintf("%v", n.IsDataReady))
    buf.WriteString(", isActiveMaster: ")
    buf.WriteString(fmt.Sprintf("%v", n.IsActiveMaster))
    buf.WriteString(", brokerName: ")
    buf.WriteString(fmt.Sprintf("%v", n.BrokerName))
    buf.WriteString(", brokerId: ")
    buf.WriteString(fmt.Sprintf("%v", n.BrokerId))
    buf.WriteString(", broker-addr: ")
    buf.WriteString(fmt.Sprintf("%v", n.BrokerCommunicationAddr))
    buf.WriteString("\n")

    return buf.String()
}