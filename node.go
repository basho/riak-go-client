package riak

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Node states

const (
	NODE_ERROR state = iota
	NODE_CREATED
	NODE_RUNNING
	NODE_HEALTH_CHECKING
	NODE_SHUTTING_DOWN
	NODE_SHUTDOWN
)

// TODO auth
type NodeOptions struct {
	RemoteAddress       string
	MinConnections      uint16
	MaxConnections      uint16
	IdleTimeout         time.Duration
	ConnectTimeout      time.Duration
	RequestTimeout      time.Duration
	HealthCheckInterval time.Duration
	HealthCheckBuilder  CommandBuilder
}

type Node struct {
	addr                *net.TCPAddr
	minConnections      uint16
	maxConnections      uint16
	idleTimeout         time.Duration
	connectTimeout      time.Duration
	requestTimeout      time.Duration
	healthCheckInterval time.Duration
	healthCheckBuilder  CommandBuilder
	// Health Check stop channel / timer
	stop         chan bool
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

	if resolvedAddress, err := net.ResolveTCPAddr("tcp", options.RemoteAddress); err == nil {
		n := &Node{
			stop:                make(chan bool),
			addr:                resolvedAddress,
			minConnections:      options.MinConnections,
			maxConnections:      options.MaxConnections,
			idleTimeout:         options.IdleTimeout,
			connectTimeout:      options.ConnectTimeout,
			requestTimeout:      options.RequestTimeout,
			healthCheckInterval: options.HealthCheckInterval,
			healthCheckBuilder:  options.HealthCheckBuilder,
			available:           make([]*connection, 0, options.MinConnections),
		}
		n.setStateDesc("NODE_ERROR", "NODE_CREATED", "NODE_RUNNING", "NODE_HEALTH_CHECKING", "NODE_SHUTTING_DOWN", "NODE_SHUTDOWN")
		n.setState(NODE_CREATED)
		return n, nil
	} else {
		return nil, err
	}
}

// exported funcs

func (n *Node) String() string {
	return fmt.Sprintf("%v|%d", n.addr, n.currentNumConnections)
}

func (n *Node) Start() (err error) {
	if err = n.stateCheck(NODE_CREATED); err != nil {
		return
	}

	logDebug("[Node] (%v) starting", n)

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

	n.setState(NODE_RUNNING)

	logDebug("[Node] (%v) started", n)

	// TODO emit stateChange event, do we care?
	return
}

func (n *Node) Stop() (err error) {
	if err = n.stateCheck(NODE_RUNNING, NODE_HEALTH_CHECKING); err != nil {
		return
	}
	n.setState(NODE_SHUTTING_DOWN)
	n.stop <- true
	n.expireTicker.Stop()
	logDebug("[Node] (%v) shutting down.", n)
	n.shutdown()
	return
}

func (n *Node) Execute(cmd Command) (executed bool, err error) {
	executed = false

	if err = n.stateCheck(NODE_RUNNING, NODE_HEALTH_CHECKING); err != nil {
		return
	}

	if n.isCurrentState(NODE_RUNNING) {
		var conn *connection
		if conn = n.getAvailableConnection(); conn == nil {
			// TODO retry?
			n.connMtx.RLock()
			defer n.connMtx.RUnlock()
			if n.currentNumConnections < n.maxConnections {
				if conn, err = n.createNewConnection(nil, true); conn == nil || err != nil {
					logErr(err)
					executed = false
					n.doHealthCheck()
					return
				}
			} else {
				logDebug("[Node] node (%v): all connections in use and at max", n)
				executed = false
				return
			}
			n.connMtx.RUnlock()
		}

		if conn == nil {
			// Should never happen
			panic(fmt.Sprintf("[Node] (%v) expected connection", n))
		}

		// TODO handle errors like connection closed / timeout
		// with regard to re-execution of command
		logDebug("[Node] (%v) - executing command '%v'", n, cmd.Name())
		defer n.returnConnectionToPool(conn, true)
		if err = conn.execute(cmd); err == nil {
			executed = true
		} else {
			// TODO basically, this is _connectionClosed in Node.js client
			executed = false
			n.doHealthCheck()
			// TODO retry command if retries remain by calling n.Execute
			// after decrementing # of tries.
		}
	}

	return
}

// non-exported funcs

func (n *Node) getAvailableConnection() (c *connection) {
	n.connMtx.Lock()
	defer n.connMtx.Unlock()
	c = nil
	if len(n.available) > 0 {
		c = n.available[0]
		n.available = n.available[1:]
	}
	return
}

func (n *Node) returnConnectionToPool(c *connection, shouldLock bool) {
	if shouldLock {
		n.connMtx.Lock()
		defer n.connMtx.Unlock()
	}
	if n.isStateLessThan(NODE_SHUTTING_DOWN) {
		c.inFlight = false
		// TODO c.resetBuffer()
		n.available = append(n.available, c)
		logDebug("[Node] (%v)|Number of avail connections: %d", n, len(n.available))
	} else {
		logDebug("[Node] (%v)|Connection returned to pool during shutdown.", n)
		n.currentNumConnections--
		c.close() // NB: discard error
	}
}

func (n *Node) shutdown() (err error) {
	n.connMtx.Lock()
	defer n.connMtx.Unlock()

	for i, conn := range n.available {
		n.available[i] = nil
		n.currentNumConnections--
		err = conn.close()
	}
	if err != nil {
		n.setState(NODE_ERROR)
		return
	}

	if n.currentNumConnections == 0 {
		n.available = nil
		n.setState(NODE_SHUTDOWN)
		logDebug("[Node] (%v) shut down.", n)
	} else {
		// Should never happen
		panic(fmt.Sprintf("[Node] (%v); Connections still in use.", n))
	}

	return
}

func (n *Node) doHealthCheck() {
	// NB: ensure we're not already health checking or shutting down
	if tmpErr := n.stateCheck(NODE_HEALTH_CHECKING, NODE_SHUTTING_DOWN); tmpErr == nil {
		logDebug("[Node] (%v) already health checking.")
	} else {
		n.setState(NODE_HEALTH_CHECKING)
		go n.healthCheck()
	}
}

func (n *Node) createNewConnection(healthCheck Command, shouldLock bool) (conn *connection, err error) {
	connectionOptions := &connectionOptions{
		remoteAddress:  n.addr,
		connectTimeout: n.connectTimeout,
		requestTimeout: n.requestTimeout,
		healthCheck:    healthCheck,
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
	if n.healthCheckBuilder != nil {
		hc = n.healthCheckBuilder.Build()
	} else {
		hc = &PingCommand{}
	}
	return
}

// private goroutine funcs

func (n *Node) healthCheck() {

	logDebug("[Node] (%v) running health check", n)

	healthCheckTicker := time.NewTicker(n.healthCheckInterval)
	defer healthCheckTicker.Stop()
	healthCheck := n.getHealthCheckCommand()

	for {
		select {
		case <-n.stop:
			logDebug("[Node] (%v) health check routine quitting.")
			return
		case t := <-healthCheckTicker.C:
			logDebug("[Node] (%v) running health check at %v", n, t)
			if conn, err := n.createNewConnection(healthCheck, true); conn == nil || err != nil {
				logDebug("[Node] (%v) failed healthcheck - conn: %v err: %v", n, conn == nil, err)
			} else {
				n.returnConnectionToPool(conn, true)
				n.setState(NODE_RUNNING)
				logDebug("[Node] (%v) healthcheck success", n)
				return
			}
		}
	}

	return
}

func (n *Node) expireIdleConnections() {
	logDebug("[Node] (%v) idle connection expiration routine starting", n)
	for {
		select {
		case <-n.stop:
			logDebug("[Node] (%v) idle connection expiration routine quitting", n)
			return
		case t := <-n.expireTicker.C:
			logDebug("[Node] (%v) expiring idle connections at %v", n, t)
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
					conn.close()
					count++
				} else {
					i++
				}
			}
			n.connMtx.Unlock()
			logDebug("[Node] (%v) expired %d connections.", n, count)
		}
	}
}
