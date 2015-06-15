package riak

import (
	"fmt"
	"net"
	"time"
	"sync"
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
	sync.RWMutex
	addr                  *net.TCPAddr
	minConnections        uint16
	maxConnections        uint16
	idleTimeout           time.Duration
	connectTimeout        time.Duration
	requestTimeout        time.Duration
	healthCheckBuilder    CommandBuilder
	available             []*connection
	currentNumConnections uint16
	state                 state
}

type state byte

const (
	CREATED state = iota
	RUNNING
	HEALTH_CHECKING
	SHUTTING_DOWN
	SHUTDOWN
)

func (v state) String() (rv string) {
	switch v {
	case CREATED:
		rv = "CREATED"
	case RUNNING:
		rv = "RUNNING"
	case HEALTH_CHECKING:
		rv = "HEALTH_CHECKING"
	case SHUTTING_DOWN:
		rv = "SHUTTING_DOWN"
	case SHUTDOWN:
		rv = "SHUTDOWN"
	}
	return
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
			state:              CREATED,
		}, nil
	} else {
		return nil, err
	}
}

// exported funcs

func (n *Node) Start() (err error) {
	if err = n.stateCheck(CREATED); err != nil {
		return
	}
	var i uint16
	for i = 0; i < n.minConnections; i++ {
		if conn, err := n.createNewConnection(); err == nil {
			n.available[i] = conn
		}
	}
	// TODO _expireTimer
	n.setState(RUNNING)
	// TODO emit stateChange event
	return
}

func (n *Node) Stop() (err error) {
	if err = n.stateCheck(CREATED, HEALTH_CHECKING); err != nil {
		return
	}
	// TODO stop expire timer
	n.setState(SHUTTING_DOWN)
    logDebug("[RiakNode] (%v) shutting down.", n.addr)
	n.shutdown()
	return
}

// non-exported funcs

func (n *Node) shutdown() (err error) {
	return
}

func (n *Node) setState(s state) {
	n.Lock()
	defer n.Unlock()
	n.state = s
	return
}

func (n *Node) stateCheck(allowed ...state) (err error) {
	n.RLock()
	defer n.RUnlock()
	stateChecked := false
	for _,s := range allowed {
		if n.state == s {
			stateChecked = true
			break
		}
	}
	if !stateChecked {
		err = fmt.Errorf("[RiakNode]: Illegal State; required %s: current: %s", allowed, n.state)
	}
	return
}

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
