package main


type SimpleDiscoveryPlugin struct {

}

func (s *SimpleDiscoveryPlugin) Ping (url string, options map[string]interface{}) (valid bool, err error) {

    return false, nil
}

func (s *SimpleDiscoveryPlugin) ElectMaster (params map[string]interface{}) (brokerId string, err error) {

    return "", nil
}


func NewSimpleDiscoveryPlugin () *SimpleDiscoveryPlugin {
    return nil
}