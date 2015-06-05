package riak

import (
	"time"
)

const defaultRemoteAddress = "127.0.0.1:8087"
const defaultMinConnections = 1
const defaultMaxConnections = 8096
const defaultIdleTimeout = time.Second * 3
const defaultConnectTimeout = time.Second * 30
const defaultRequestTimeout = time.Second * 5
