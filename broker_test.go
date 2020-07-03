package quebroker

import (
	"regexp"
	"strings"
	"testing"
)

func TestBrokerInstanceFromTomlConfig(t *testing.T) {
	_h1 := "[TestBrokerInstanceFromTomlConfig]"
	_instance, err := BrokerInstanceFromTomlConfig()

	if err != nil {
		t.Errorf("%v error => %v\n", _h1, err)
	}
	// random check on some expected values
	if strings.Compare(_instance.Name, "broker_server_1") != 0 {
		t.Errorf("%v[broker.name] expected [%v] BUT got [%v]", _h1, "broker_server_1", _instance.Name)
	}
	if len(_instance.BootstrapServers) != 2 {
		t.Errorf("%v[boostrapServer length] expected [%v] BUT got [%v]", _h1, 2, len(_instance.BootstrapServers))
	}
	if strings.Compare(_instance.Cluster.Name, "devCluster") != 0 {
		t.Errorf("%v[cluster.name] expected [%v] BUT got [%v]", _h1, "devCluster", _instance.Cluster.Name)
	}
	if _instance.Network.Port != 6801 {
		t.Errorf("%v[network.port] expected [%v] BUT got [%v]", _h1, 6801, _instance.Network.Port)
	}
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

// testing on the correct regexp to extract ${ENV_VAR} tokens
func TestBrokerExtraEnvParam(t *testing.T) {
	_h1 := "[TestBrokerExtraEnvParam]"
	t.Skip(_h1, "[OPTIONAL] testing on the correct regexp to extract ${ENV_VAR} tokens")

	_cases := []struct {
		input    string
		expected []string
	}{
		{"${HOME}/abc", []string{"${HOME}"}},
		{"${HOME}/${BROKER_FILE}", []string{"${HOME}", "${BROKER_FILE}"}},
		{"/tmp/logs/${LOGS-DIR}/avro/${LOG_TYPE}", []string{"${LOGS-DIR}", "${LOG_TYPE}"}},
	}
	_re, err := regexp.Compile(`\$\{[a-z|A-Z|_|-]+\}`)
	if err != nil {
		t.Errorf("%v compile regexp failed: %v", _h1, err)
	}

	// define a function for []string / slice equality check
	_arrayCompareFunc := func(a1, a2 []string) (match bool) {
		match = true
		// a. nil check
		if (a1 == nil && a2 != nil) || (a1 != nil && a2 == nil) {
			match = false
			return
		}
		// b. length check
		if len(a1) != len(a2) {
			match = false
			return
		}
		// c. element level check
		for _i, _aVal := range a1 {
			_a2Val := a2[_i]
			if _aVal != _a2Val {
				match = false
				return
			}
		}
		return
	}

	// loop through
	for _, _case := range _cases {
		_matches := _re.FindAllString(_case.input, -1)
		if !_arrayCompareFunc(_matches, _case.expected) {
			t.Errorf("%v patterns don't match, expected [%v] but got [%v]", _h1, _case.expected, _matches)
		}
	}
}
