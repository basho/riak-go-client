package riak

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Constants identifying Node state
const (
	nodeError state = iota
	nodeCreated
	nodeRunning
	nodeHealthChecking
	nodeShuttingDown
	nodeShutdown
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
	minConnections      uint16
	maxConnections      uint16
	idleTimeout         time.Duration
	connectTimeout      time.Duration
	requestTimeout      time.Duration
	healthCheckInterval time.Duration
	healthCheckBuilder  CommandBuilder
	authOptions         *AuthOptions
	// Health Check stop channel / timer
	stopChan     chan bool
	expireTicker *time.Ticker
	// Connection Pool
	connMtx               sync.RWMutex
	available             []*connection
	currentNumConnections uint16
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
	if options.HealthCheckInterval == 0 {
		options.HealthCheckInterval = defaultHealthCheckInterval
	}

	resolvedAddress, err := net.ResolveTCPAddr("tcp", options.RemoteAddress)
	if err == nil {
		n := &Node{
			stopChan:            make(chan bool),
			addr:                resolvedAddress,
			minConnections:      options.MinConnections,
			maxConnections:      options.MaxConnections,
			idleTimeout:         options.IdleTimeout,
			connectTimeout:      options.ConnectTimeout,
			requestTimeout:      options.RequestTimeout,
			healthCheckInterval: options.HealthCheckInterval,
			healthCheckBuilder:  options.HealthCheckBuilder,
			authOptions:         options.AuthOptions,
			available:           make([]*connection, 0, options.MinConnections),
		}
		n.setStateDesc("nodeError", "nodeCreated", "nodeRunning", "nodeHealthChecking", "nodeShuttingDown", "nodeShutdown")
		n.setState(nodeCreated)
		return n, nil
	}

	return nil, err
}

// String returns a formatted string including the remoteAddress for the Node and its current
// connection count in the pool
func (n *Node) String() string {
	return fmt.Sprintf("%v|%d", n.addr, n.currentNumConnections)
}

// Start opens a connection with Riak at the configured remoteAddress and adds the connections to the
// active pool
func (n *Node) start() (err error) {
	if err = n.stateCheck(nodeCreated); err != nil {
		return
	}

	logDebug("[Node]", "(%v) starting", n)

	var i uint16
	for i = 0; i < n.minConnections; i++ {
		if conn, err := n.createNewConnection(nil, false); err == nil {
			if conn == nil {
				// Should never happen
				panic(fmt.Sprintf("[Node] (%v) could not create connection in Start", n))
			} else {
				n.returnConnectionToPool(conn, false)
			}
		} else {
			break
		}
	}

	if err != nil {
		return
	}

	n.expireTicker = time.NewTicker(thirtySeconds)
	go n.expireIdleConnections()

	n.setState(nodeRunning)
	logDebug("[Node]", "(%v) started", n)
	return
}

// Stop closes the connections with Riak at the configured remoteAddress and removes the connections
// from the active pool
func (n *Node) stop() (err error) {
	if err = n.stateCheck(nodeRunning, nodeHealthChecking); err != nil {
		return
	}
	n.setState(nodeShuttingDown)
	n.stopChan <- true
	n.expireTicker.Stop()
	close(n.stopChan)
	logDebug("[Node]", "(%v) shutting down.", n)
	n.shutdown()
	return
}

// Execute retrieves an available connection from the pool and executes the Command operation against
// Riak
func (n *Node) execute(cmd Command) (executed bool, err error) {
	executed = false

	if err = n.stateCheck(nodeRunning, nodeHealthChecking); err != nil {
		return
	}

	if n.isCurrentState(nodeRunning) {
		var conn *connection
		if conn = n.getAvailableConnection(); conn == nil {
			n.connMtx.RLock()
			defer n.connMtx.RUnlock()
			if n.currentNumConnections < n.maxConnections {
				if conn, err = n.createNewConnection(nil, true); conn == nil || err != nil {
					logErr("[Node]", err)
					n.doHealthCheck()
					executed = false
					return
				}
			} else {
				logDebug("[Node]", "(%v): all connections in use and at max", n)
				executed = false
				return
			}
			n.connMtx.RUnlock()
		}

		if conn == nil {
			// Should never happen
			panic(fmt.Sprintf("[Node] (%v) expected connection", n))
		}

		logDebug("[Node]", "(%v) - executing command '%v'", n, cmd.Name())
		executed = true
		err = conn.execute(cmd)
		if err == nil {
			// NB: basically the success path of _responseReceived in Node.js client
			n.returnConnectionToPool(conn, true)
		} else {
			// NB: basically, this is _connectionClosed / _responseReceived in Node.js client
			// must differentiate between Riak and non-Riak errors here and within execute() in connection
			// TODO type switch?
			switch err.(type) {
			case RiakError, ClientError:
				// Riak and Client errors will not close connection
				n.returnConnectionToPool(conn, true)
			default:
				// NB: must be a non-Riak, non-Client error
				n.connMtx.Lock()
				defer n.connMtx.Unlock()
				logDebug("[Node]", "(%v) - closing connection due to non-Riak error: '%v'", n, err)
				if err := conn.close(); err != nil {
					logErr("[Node]", err)
				}
				n.currentNumConnections--
				n.doHealthCheck()
				// TODO evaluate _connectionClosed code in riaknode.js
			}
		}
	}

	return
}

func (n *Node) getAvailableConnection() *connection {
	n.connMtx.Lock()
	defer n.connMtx.Unlock()
	if len(n.available) > 0 {
		c := n.available[0]
		if c.available() {
			n.available = n.available[1:]
			return c
		}
	}
	return nil
}

func (n *Node) returnConnectionToPool(c *connection, shouldLock bool) {
	if shouldLock {
		n.connMtx.Lock()
		defer n.connMtx.Unlock()
	}
	if n.isStateLessThan(nodeShuttingDown) {
		// TODO c.resetBuffer()
		n.available = append(n.available, c)
		logDebug("[Node]", "(%v)|Number of avail connections: %d", n, len(n.available))
	} else {
		logDebug("[Node]", "(%v)|Connection returned to pool during shutdown.", n)
		n.currentNumConnections--
		c.close() // NB: discard error
	}
}

func (n *Node) shutdown() (err error) {
	n.connMtx.Lock()
	defer n.connMtx.Unlock()

	allClosed := false
	for allClosed == false {
		if n.currentNumConnections != uint16(len(n.available)) {
			logError("[Node]", "shutdown: current connection count '%d' does NOT equal pool length '%d'", n.currentNumConnections, len(n.available))
		}
		for i, conn := range n.available {
			n.available[i] = nil
			n.currentNumConnections--
			if conn != nil {
				err = conn.close()
			}
		}
		if err != nil {
			n.setState(nodeError)
			return
		}

		if n.currentNumConnections == 0 {
			allClosed = true
			n.available = nil
			n.setState(nodeShutdown)
			logDebug("[Node]", "(%v) shut down.", n)
		} else {
			logWarn("[Node]", "(%v) %d connections still in use.", n, n.currentNumConnections)
		}
	}

	return
}

func (n *Node) doHealthCheck() {
	// NB: ensure we're not already health checking or shutting down
	if tmpErr := n.stateCheck(nodeHealthChecking, nodeShuttingDown); tmpErr == nil {
		logDebug("[Node]", "(%v) is already health checking or shutting down.", n)
	} else {
		n.setState(nodeHealthChecking)
		go n.healthCheck()
	}
}

func (n *Node) createNewConnection(healthCheck Command, shouldLock bool) (conn *connection, err error) {
	connectionOptions := &connectionOptions{
		remoteAddress:  n.addr,
		connectTimeout: n.connectTimeout,
		requestTimeout: n.requestTimeout,
		healthCheck:    healthCheck,
		authOptions:    n.authOptions,
	}
	if conn, err = newConnection(connectionOptions); err == nil {
		if err = conn.connect(); err == nil {
			if shouldLock {
				n.connMtx.Lock()
				defer n.connMtx.Unlock()
			}
			n.currentNumConnections++
			return
		}
	}
	return
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
	if tmpErr := n.stateCheck(nodeHealthChecking); tmpErr != nil {
		logDebug("[Node]", "(%v) expected to be in health checking state.", n)
		return false
	}

	// ensure we're not shutting down
	if tmpErr := n.stateCheck(nodeShuttingDown); tmpErr == nil {
		logDebug("[Node]", "(%v) is shutting down.", n)
		return false
	} else {
		return true
	}
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
					if conn, err := n.createNewConnection(healthCheckCommand, true); conn == nil || err != nil {
						logDebug("[Node]", "(%v) failed healthcheck - conn: %v err: %v", n, conn == nil, err)
					} else {
						n.returnConnectionToPool(conn, true)
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

func (n *Node) expireIdleConnections() {
	logDebug("[Node]", "(%v) idle connection expiration routine is starting", n)
	for {
		select {
		case <-n.stopChan:
			logDebug("[Node]", "(%v) idle connection expiration routine is quitting", n)
			return
		case t := <-n.expireTicker.C:
			// NB: ensure we're not already shutting down
			if tmpErr := n.stateCheck(nodeShuttingDown); tmpErr == nil {
				logDebug("[Node]", "(%v) shutting down, idle connection expiration routine is quitting.")
				return
			} else {
				logDebug("[Node]", "(%v) expiring idle connections at %v", n, t)
				n.connMtx.Lock()
				count := 0
				now := time.Now()
				for i := 0; i < len(n.available); {
					if n.currentNumConnections <= n.minConnections {
						break
					}
					conn := n.available[i]
					if now.Sub(conn.lastUsed) >= n.idleTimeout {
						// NB: overwrites current element in slice with last element,
						// and shrinks the slice by one
						// does NOT increment i so that we re-visit the index, which now
						// contains what used to be the last element
						// "Delete without preserving order"
						// https://github.com/golang/go/wiki/SliceTricks
						l := len(n.available) - 1
						n.available[i], n.available[l], n.available =
							n.available[l], nil, n.available[:l]
						n.currentNumConnections--
						conn.close() // TODO log error?
						count++
					} else {
						i++
					}
				}
				n.connMtx.Unlock()
				logDebug("[Node]", "(%v) expired %d connections.", n, count)
			}
		}
	}
}
