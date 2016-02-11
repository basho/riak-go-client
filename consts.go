package riak

import (
	"fmt"
	"time"
)

const (
	threeSeconds                  = time.Second * 3
	fiveSeconds                   = time.Second * 5
	tenSeconds                    = time.Second * 10
	defaultBucketType             = "default"
	defaultRemotePort             = uint16(8087)
	defaultMinConnections         = uint16(1)
	defaultMaxConnections         = uint16(256)
	defaultIdleExpirationInterval = fiveSeconds
	defaultIdleTimeout            = tenSeconds
	defaultConnectTimeout         = threeSeconds
	defaultRequestTimeout         = fiveSeconds
	defaultHealthCheckInterval    = 125 * time.Millisecond
	defaultExecutionAttempts      = byte(3)
	defaultQueueExecutionInterval = 125 * time.Millisecond
	defaultInitBuffer             = 2048
)

var defaultRemoteAddress = fmt.Sprintf("127.0.0.1:%d", defaultRemotePort)
