package main

// *******************************
// **   broker-config related   **
// *******************************

// environment variable pointing to the path of the que broker config file
const envVarBrokerConfigPath = "QUE_BROKER_CONFIG_PATH"

// the path containing the config files under the current user's home directory
const homeDirectoryConfigDir = ".que"

// local broker config file (the relative path)
const localBrokerConfigPath = "queBroker.toml"



// ***************************
// **   discovery related   **
// ***************************

// key representing the discovery seed list;
// value could be []string or any interface
const KeyDiscoverySeedList = "KeyDiscoverySeedList"

// key representing the cluster name (discovery module);
// value could be string
const KeyDiscoveryClusterName = "KeyDiscoveryClusterName"

// key representing the logger instance (for discovery module);
// value MUST be a valid implementation of queutil.FlexLogger
const KeyDiscoveryLogger = "KeyDiscoveryLogger"

// key representing the security-scheme name (for discovery module);
// value MUST be a valid string (should be denoted by a constant as well e.g. BASIC)
const KeyDiscoverySecurityScheme = "KeyDiscoverySecurityScheme"


// the signal for a channel to start create or join a cluster
const ChanSignalCreateOrJoinCluster = 1


// ***********************
// **   http related    **
// ***********************

// http content type => json
const httpContentTypeJson = "application/json"