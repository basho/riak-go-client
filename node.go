package riak

import (
	"fmt"
	"net"
	"time"
)

// Constants identifying Node state
const (
	nodeCreated state = iota
	nodeRunning
	nodeHealthChecking
	nodeShuttingDown
	nodeShutdown
	nodeError
)

// NodeOptions defines the RemoteAddress and operational configuration for connections to a Riak KV
// instance
type NodeOptions struct {
	RemoteAddress       string
	MinConnections      uint16
	MaxConnections      uint16
	IdleTimeout         time.Duration
	ConnectTimeout      time.Duration
	RequestTimeout      time.Duration
	HealthCheckInterval time.Duration
	HealthCheckBuilder  CommandBuilder
	AuthOptions         *AuthOptions
}

// Node is a struct that contains all of the information needed to connect and maintain connections
// with a Riak KV instance
type Node struct {
	addr                *net.TCPAddr
	healthCheckInterval time.Duration
	healthCheckBuilder  CommandBuilder
	authOptions         *AuthOptions
	stopChan            chan bool
	// Health Check
	expireTicker *time.Ticker
	// Connections
	cm *connectionManager
	// State
	stateData
}

var defaultNodeOptions = &NodeOptions{
	RemoteAddress:  defaultRemoteAddress,
	MinConnections: defaultMinConnections,
	MaxConnections: defaultMaxConnections,
	IdleTimeout:    defaultIdleTimeout,
	ConnectTimeout: defaultConnectTimeout,
	RequestTimeout: defaultRequestTimeout,
}

// NewNode is a factory function that takes a NodeOptions struct and returns a Node struct
func NewNode(options *NodeOptions) (*Node, error) {
	if options == nil {
		options = defaultNodeOptions
	}
	if options.RemoteAddress == "" {
		options.RemoteAddress = defaultRemoteAddress
	}
	if options.HealthCheckInterval == 0 {
		options.HealthCheckInterval = defaultHealthCheckInterval
	}

	var err error
	var resolvedAddress *net.TCPAddr
	resolvedAddress, err = net.ResolveTCPAddr("tcp", options.RemoteAddress)
	if err == nil {
		stopChan := make(chan bool)
		n := &Node{
			stopChan:            stopChan,
			addr:                resolvedAddress,
			healthCheckInterval: options.HealthCheckInterval,
			healthCheckBuilder:  options.HealthCheckBuilder,
			authOptions:         options.AuthOptions,
		}

		connMgrOpts := &connectionManagerOptions{
			addr:           resolvedAddress,
			stopChan:       stopChan,
			minConnections: options.MinConnections,
			maxConnections: options.MaxConnections,
			idleTimeout:    options.IdleTimeout,
			connectTimeout: options.ConnectTimeout,
			requestTimeout: options.RequestTimeout,
		}

		var cm *connectionManager
		if cm, err = newConnectionManager(connMgrOpts); err == nil {
			n.cm = cm
			n.initStateData("nodeError", "nodeCreated", "nodeRunning", "nodeHealthChecking", "nodeShuttingDown", "nodeShutdown")
			n.setState(nodeCreated)
			return n, nil
		}
	}

	return nil, err
}

// String returns a formatted string including the remoteAddress for the Node and its current
// connection count
func (n *Node) String() string {
	return fmt.Sprintf("%v|%d", n.addr, n.cm.count())
}

// Start opens a connection with Riak at the configured remoteAddress and adds the connections to the
// active pool
func (n *Node) start() error {
	if err := n.stateCheck(nodeCreated); err != nil {
		return err
	}

	logDebug("[Node]", "(%v) starting", n)
	n.cm.start()
	n.setState(nodeRunning)
	logDebug("[Node]", "(%v) started", n)

	return nil
}

// Stop closes the connections with Riak at the configured remoteAddress and removes the connections
// from the active pool
func (n *Node) stop() error {
	if err := n.stateCheck(nodeRunning, nodeHealthChecking); err != nil {
		return err
	}

	logDebug("[Node]", "(%v) shutting down.", n)

	n.setState(nodeShuttingDown)
	n.stopChan <- true
	close(n.stopChan)

	err := n.cm.stop()

	if err == nil {
		n.setState(nodeShutdown)
		logDebug("[Node]", "(%v) shut down.", n)
	} else {
		n.setState(nodeError)
	}

	return err
}

// Execute retrieves an available connection from the pool and executes the Command operation against
// Riak
func (n *Node) execute(cmd Command) (bool, error) {
	if err := n.stateCheck(nodeRunning, nodeHealthChecking); err != nil {
		return false, err
	}

	cmd.setLastNode(n)

	if n.isCurrentState(nodeRunning) {
		conn, err := n.cm.get()
		if err != nil {
			logErr("[Node]", err)
			n.doHealthCheck()
			return false, err
		}

		if conn == nil {
			panic(fmt.Sprintf("[Node] (%v) expected non-nil connection", n))
		}

		logDebug("[Node]", "(%v) - executing command '%v'", n, cmd.Name())
		err = conn.execute(cmd)
		if err == nil {
			// NB: basically the success path of _responseReceived in Node.js client
			if cmErr := n.cm.put(conn); cmErr != nil {
				logErr("[Node]", cmErr)
			}
			return true, nil
		} else {
			// NB: basically, this is _connectionClosed / _responseReceived in Node.js client
			// must differentiate between Riak and non-Riak errors here and within execute() in connection
			// TODO type switch?
			switch err.(type) {
			case RiakError, ClientError:
				// Riak and Client errors will not close connection
				if cmErr := n.cm.put(conn); cmErr != nil {
					logErr("[Node]", cmErr)
				}
				return true, err
			default:
				// NB: must be a non-Riak, non-Client error
				if cmErr := n.cm.remove(conn); cmErr != nil {
					logErr("[Node]", cmErr)
				}
				n.doHealthCheck()
				return true, err
			}
		}
	} else {
		return false, nil
	}
}

func (n *Node) doHealthCheck() {
	// NB: ensure we're not already health checking or shutting down
	if n.isStateLessThan(nodeHealthChecking) {
		n.setState(nodeHealthChecking)
		go n.healthCheck()
	} else {
		logDebug("[Node]", "(%v) is already health checking or shutting down.", n)
	}
}

func (n *Node) getHealthCheckCommand() (hc Command) {
	// This is necessary to have a unique Command struct as part of each
	// connection so that concurrent calls to check health can all have
	// unique results
	var err error
	if n.healthCheckBuilder != nil {
		hc, err = n.healthCheckBuilder.Build()
	} else {
		hc = &PingCommand{}
	}

	if err != nil {
		logErr("[Node]", err)
		hc = &PingCommand{}
	}

	return
}

func (n *Node) ensureHealthCheckCanContinue() bool {
	// ensure we ARE health checking
	if !n.isCurrentState(nodeHealthChecking) {
		logDebug("[Node]", "(%v) expected health checking state, got %s", n, n.stateData.String())
		return false
	}
	return true
}

// private goroutine funcs

func (n *Node) healthCheck() {

	logDebug("[Node]", "(%v) running health check", n)

	healthCheckTicker := time.NewTicker(n.healthCheckInterval)
	defer healthCheckTicker.Stop()
	healthCheckCommand := n.getHealthCheckCommand()

	for {
		if n.ensureHealthCheckCanContinue() {
			select {
			case <-n.stopChan:
				logDebug("[Node]", "(%v) health check quitting", n)
				return
			case t := <-healthCheckTicker.C:
				if n.ensureHealthCheckCanContinue() {
					logDebug("[Node]", "(%v) running health check at %v", n, t)
					if conn, err := n.cm.create(healthCheckCommand); err != nil {
						logDebug("[Node]", "(%v) failed healthcheck, err: %v", n, err)
					} else {
						n.cm.put(conn)
						n.setState(nodeRunning)
						logDebug("[Node]", "(%v) healthcheck success", n)
						return
					}
				}
			}
		} else {
			return
		}
	}
}
