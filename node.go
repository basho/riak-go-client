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
	available      []*connection
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

		minConnections := options.MinConnections

		connectionOptions := &connectionOptions{
			remoteAddress:  resolvedAddress,
			connectTimeout: options.ConnectTimeout,
			requestTimeout: options.RequestTimeout,
			healthCheck:    options.HealthCheck,
		}

		available := make([]*connection, minConnections)
		for i := 0; i < len(available); i++ {
			if available[i], err = newConnection(connectionOptions); err != nil {
				return nil, err
			}
		}

		return &Node{
			addr:           resolvedAddress,
			minConnections: minConnections,
			maxConnections: options.MaxConnections,
			idleTimeout:    options.IdleTimeout,
			available:      available,
		}, nil
	} else {
		return nil, err
	}
}
