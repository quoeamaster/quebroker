package main

import (
    "github.com/quoeamaster/queutil"
    "fmt"
    "net/http"
    "bytes"
    "strings"
    "time"
)

type SimpleDiscoveryPlugin struct {

}

// implementation on the join cluster operation:
// 1. get back the simple discovery seed list
// 2. per member, do a handshake / ping; provide the following a) cluster_name, b) seed member's address
// 3. this method should return
//  a) bool (indicating valid to join or not e.g. same cluster?)
//  b) member's roles (e.g. master ready or other roles) (affect election; only master-ready broker can join election)
//  c) any known active master on the seed member's side (affect election; if already got an active, no need to run election)
//
// for this implementation; the seed list member(s) are visited 1 by 1; and would join the cluster once a member is handshake-d.
// sniffing (finding all members in the cluster) and handshake are basically 2 types of operations
func (s *SimpleDiscoveryPlugin) Ping (url string, options map[string]interface{}) (valid bool, returnValues map[string]interface{}, err error) {
    var seedList []string
    var clusterName string
    var logger *queutil.FlexLogger
    var restClient *http.Client
    var securityScheme string

    defer func() {
        if r := recover(); r != nil {
            switch r.(type) {
            case error:
                // cast back to error and return the caller
                err = r.(error)
            default:
                if logger != nil {
                    logger.Err([]byte(fmt.Sprintf("[discovery] failed to Ping; reason: %v\n", r)))
                }
                // an unknown error; can't "hide" it
                panic(r)
            }
        }   // r is non-null
    }()

    valid = false
    err = nil
    returnValues = make(map[string]interface{})

    // get back the parameter(s) for the ping
    if options != nil {
        // get logger first
        if val := options[KeyDiscoveryLogger]; val != nil {
            logger = val.(*queutil.FlexLogger)
        }
        // get seedList []string
        if val := options[KeyDiscoverySeedList]; val != nil {
            seedList = val.([]string)
        }
        // get clusterName
        if val := options[KeyDiscoveryClusterName]; val != nil {
            clusterName = val.(string)
        }
        // security scheme
        if val := options[KeyDiscoverySecurityScheme]; val != nil {
            securityScheme = val.(string)
        }
    }

    // for SimpleDiscovery; just call the corresponding REST api would do
    // (other discovery modules might have their own way to ping cluster members)
    pingTimeout, err := queutil.CreateTimeoutByString("5s")
    if err != nil {
        return valid, returnValues, err
    }
    restClient = queutil.GenerateHttpClient(pingTimeout, nil, nil, nil)

    canBreakRetry := false
    maxRetryForAllSeeds := 2
    for idx := 0; idx < maxRetryForAllSeeds; idx++ {
        if canBreakRetry {
            // break the outer loop (retry loop)
            break
        }
        // wait for 2 seconds before a retry to connect
        if idx > 0 {
            time.Sleep(time.Millisecond * 2000)
        }

        for _, seed := range seedList {

            // avoid pinging itself... itself MUST always be join-able (common sense)
            if strings.Compare(url, seed) == 0 {
                continue
            }
            handshakeUrl := queutil.BuildGenericApiUrl(seed, securityScheme, "_network/_handshake")
            bJsonBody := s.buildHandShakeJsonBody(clusterName, seed)
            // do handshake (call the corresponding broker's _network/_handshake endpoint
            res, err := restClient.Post(handshakeUrl, httpContentTypeJson, &bJsonBody)
            if err != nil {
                // retry on the next round (give the target broker a chance)
                if logger != nil {
                    logger.Info([]byte(fmt.Sprintf("[discovery] failed to connect [%v], retry again...\n", seed)))
                }
            } else {

                bArr, err := queutil.GetHttpResponseContent(res)
                if err != nil {
                    logger.Warn([]byte(fmt.Sprintf("[discovery] failed to read the response from [%v], error => [%v]\n", seed, err.Error())))
                    continue
                }

                // update the return values to include the []byte of the response from network/_handshake api
                returnValues[KeyDiscoveryHandshakeResponseByteArray]  = bArr

                // only 200 status is ok to join; 202 is everything is fine except can't join the cluster together
                if res.StatusCode == 200 {
                    canBreakRetry = true
                }
                // close the response as should not have any further usage...
                res.Body.Close()

                if canBreakRetry {
                    // can break means already found 1 broker connectible
                    valid = true
                    // break the inner loop (seedList)
                    break
                }
            }   // end -- if err (valid)
        }
    }
    return valid, returnValues, err
}

func (s *SimpleDiscoveryPlugin) buildHandShakeJsonBody (clusterName, seed string) bytes.Buffer {
    var b bytes.Buffer

    b = queutil.BeginJsonStructure(b)
    b = queutil.AddStringToJsonStructure(b, "clusterName", clusterName)
    b = queutil.AddStringToJsonStructure(b, "seedIP", seed)
    b = queutil.EndJsonStructure(b)

    return b
}


func (s *SimpleDiscoveryPlugin) ElectMaster (params map[string]interface{}) (brokerId string, err error) {

    return "", nil
}


// method to create an instance of SimpleDiscoveryPlugin
func NewSimpleDiscoveryPlugin () *SimpleDiscoveryPlugin {
    return new(SimpleDiscoveryPlugin)
}