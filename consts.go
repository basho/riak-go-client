package riak

import (
	"time"
)

const (
	thirtySeconds = time.Second * 30
	thirtyMinutes = time.Minute * 30
)

const defaultRemoteAddress = "127.0.0.1:8087"
const defaultMinConnections = 1
const defaultMaxConnections = 8096
const defaultIdleTimeout = time.Second * 3
const defaultConnectTimeout = time.Second * 30
const defaultRequestTimeout = time.Second * 5
const defaultHealthCheckInterval = time.Second * 5

const defaultBucketType = "default"
