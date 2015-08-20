package riak

import (
	"time"
)

const (
	thirtySeconds = time.Second * 30
	thirtyMinutes = time.Minute * 30
)

const (
	defaultRemoteAddress          = "127.0.0.1:8087"
	defaultMinConnections         = uint16(1)
	defaultMaxConnections         = uint16(8096)
	defaultIdleTimeout            = time.Second * 3
	defaultConnectTimeout         = time.Second * 30
	defaultRequestTimeout         = time.Second * 5
	defaultHealthCheckInterval    = time.Second
	defaultExecutionAttempts      = byte(3)
	defaultQueueExecutionInterval = time.Second

	defaultBucketType = "default"
)
