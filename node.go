package riak

import (
	"net"
	"time"
)

// TODO auth
type NodeOptions struct {
	RemoteAddress      string
	MinConnections     uint16
	MaxConnections     uint16
	IdleTimeout        time.Duration
	ConnectTimeout     time.Duration
	RequestTimeout     time.Duration
	HealthCheckBuilder CommandBuilder
}

type Node struct {
	addr                  *net.TCPAddr
	minConnections        uint16
	maxConnections        uint16
	idleTimeout           time.Duration
	connectTimeout        time.Duration
	requestTimeout        time.Duration
	healthCheckBuilder    CommandBuilder
	available             []*connection
	currentNumConnections uint16
}

var defaultNodeOptions = &NodeOptions{
	RemoteAddress:  defaultRemoteAddress,
	MinConnections: defaultMinConnections,
	MaxConnections: defaultMaxConnections,
	IdleTimeout:    defaultIdleTimeout,
	ConnectTimeout: defaultConnectTimeout,
	RequestTimeout: defaultRequestTimeout,
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
			addr:               resolvedAddress,
			minConnections:     options.MinConnections,
			maxConnections:     options.MaxConnections,
			idleTimeout:        options.IdleTimeout,
			connectTimeout:     options.ConnectTimeout,
			requestTimeout:     options.RequestTimeout,
			healthCheckBuilder: options.HealthCheckBuilder,
			available:          make([]*connection, options.MinConnections),
		}, nil
	} else {
		return nil, err
	}
}

// exported funcs

func (n *Node) Start() (err error) {
	// TODO _stateCheck - needed?
	var i uint16
	for i = 0; i < n.minConnections; i++ {
		if conn, err := n.createNewConnection(); err == nil {
			n.available[i] = conn
		}
	}
	// TODO _expireTimer
	// TODO State.RUNNING
	// TODO emit stateChange event
	return
}

// non-exported funcs

func (n *Node) createNewConnection() (conn *connection, err error) {
	connectionOptions := &connectionOptions{
		remoteAddress:  n.addr,
		connectTimeout: n.connectTimeout,
		requestTimeout: n.requestTimeout,
	}

	// This is necessary to have a unique Command struct as part of each
	// connection so that concurrent calls to check health can all have
	// unique results
	if n.healthCheckBuilder != nil {
		connectionOptions.healthCheck = n.healthCheckBuilder.Build()
	}

	if conn, err = newConnection(connectionOptions); err == nil {
		if err = conn.connect(); err == nil {
			n.currentNumConnections++
			return
		}
	}
	return
}
