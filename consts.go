package riak

import (
	"fmt"
	"time"
)

const (
	threeSeconds                  = time.Second * 3
	fiveSeconds                   = time.Second * 5
	thirtySeconds                 = time.Second * 30
	defaultBucketType             = "default"
	defaultRemotePort             = uint16(8087)
	defaultMinConnections         = uint16(1)
	defaultMaxConnections         = uint16(8096)
	defaultIdleTimeout            = threeSeconds
	defaultConnectTimeout         = thirtySeconds
	defaultRequestTimeout         = fiveSeconds
	defaultHealthCheckInterval    = 125 * time.Millisecond
	defaultExecutionAttempts      = byte(3)
	defaultQueueExecutionInterval = 125 * time.Millisecond
)

var defaultRemoteAddress = fmt.Sprintf("127.0.0.1:%d", defaultRemotePort)
