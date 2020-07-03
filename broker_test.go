package quebroker

import (
	"fmt"
	"strings"
	"testing"
)

func TestBrokerInstanceFromTomlConfig(t *testing.T) {
	_h1 := "[TestBrokerInstanceFromTomlConfig]"
	_instance, err := BrokerInstanceFromTomlConfig()

	if err != nil {
		t.Error(err)
	}
	fmt.Println("instance created", _instance.String())

	t.Errorf("%v not yet implemented completely", _h1)
}

func TestBrokerGenerateIDs(t *testing.T) {
	_h1 := "[TestBrokerGenerateIDs]"
	_targetBrokerID := "6a61736f-6e73-2d4d-4250-5f5f55736572"
	_targetClusterID := "20202020-2020-2064-6576-536572766572"

	_instance := new(Broker)
	_instance.Cluster.Name = "devServer"

	err := _instance.generateIDs()
	if err != nil {
		t.Errorf("%v[calling generateIDs] %v", _h1, err)
	}

	if strings.Compare(_instance.ID, _targetBrokerID) != 0 {
		t.Errorf("%v expected broker.id to be [%v]", _h1, _targetBrokerID)
	}
	if strings.Compare(_instance.Cluster.ID, _targetClusterID) != 0 {
		t.Errorf("%v expected cluster.id to be [%v]", _h1, _targetClusterID)
	}
}
