package quebroker

import (
	"fmt"
	"testing"
)

func TestBrokerInstanceFromTomlConfig(t *testing.T) {
	fmt.Println("B4 instance created")
	_instance, err := BrokerInstanceFromTomlConfig()

	if err != nil {
		t.Error(err)
	}
	fmt.Println("instance created", _instance)
}
