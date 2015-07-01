// +build integration

package riak

import (
	"fmt"
	"os"
	"strconv"
)

var riakHost = "riak-test"
var riakPort uint16 = 10017
var remoteAddress = "riak-test:10017"

func init() {
	if hostEnvVar := os.ExpandEnv("$RIAK_HOST"); hostEnvVar != "" {
		riakHost = hostEnvVar
	}
	if portEnvVar := os.ExpandEnv("$RIAK_PORT"); portEnvVar != "" {
		if portNum, err := strconv.Atoi(portEnvVar); err == nil {
			riakPort = uint16(portNum)
		}
	}
	remoteAddress = fmt.Sprintf("%s:%d", riakHost, riakPort)
}
