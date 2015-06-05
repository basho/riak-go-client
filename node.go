package riak

import (
	"net"
	"time"
)

// TODO auth
type NodeOptions struct {
	RemoteAddress  string
	MinConnections uint16
	MaxConnections uint16
	IdleTimeout    time.Duration
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
	HealthCheck    Command
}

type Node struct {
	addr           *net.TCPAddr
	minConnections uint16
	maxConnections uint16
	idleTimeout    time.Duration
	connectTimeout time.Duration
	requestTimeout time.Duration
	healthCheck    Command
}

var defaultNodeOptions = &NodeOptions{
	RemoteAddress:  defaultRemoteAddress,
	MinConnections: defaultMinConnections,
	MaxConnections: defaultMaxConnections,
	IdleTimeout:    defaultIdleTimeout,
	ConnectTimeout: defaultConnectTimeout,
	RequestTimeout: defaultRequestTimeout,
	HealthCheck:    &PingCommand{},
}

func NewNode(options *NodeOptions) (*Node, error) {
	if options == nil {
		options = defaultNodeOptions
	}
	if options.RemoteAddress == "" {
		options.RemoteAddress = defaultRemoteAddress
	}
	if options.MinConnections == 0 {
		options.MinConnections = defaultMinConnections
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = defaultMaxConnections
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = defaultIdleTimeout
	}
	if options.ConnectTimeout == 0 {
		options.ConnectTimeout = defaultConnectTimeout
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if resolvedAddress, err := net.ResolveTCPAddr("tcp", options.RemoteAddress); err == nil {
		return &Node{
			addr:           resolvedAddress,
			minConnections: options.MinConnections,
			maxConnections: options.MaxConnections,
			idleTimeout:    options.IdleTimeout,
			connectTimeout: options.ConnectTimeout,
			requestTimeout: options.RequestTimeout,
			healthCheck:    options.HealthCheck,
		}, nil
	} else {
		return nil, err
	}
}
