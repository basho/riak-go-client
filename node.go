package riak

import (
	"fmt"
	"net"
	"sync"
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
	stateMtx              sync.RWMutex
	connMtx               sync.Mutex
	stop                  chan bool
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
	expireTicker          *time.Ticker
}

type state byte

const (
	ERROR state = iota
	CREATED
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
			stop:               make(chan bool),
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

func (n *Node) String() string {
	return fmt.Sprintf("%v|%d", n.addr, n.currentNumConnections)
}

func (n *Node) Start() (err error) {
	if err = n.stateCheck(CREATED); err != nil {
		return
	}

	logDebug("[Node] (%v) starting", n)

	n.connMtx.Lock()
	var i uint16
	for i = 0; i < n.minConnections; i++ {
		if conn, err := n.createNewConnection(); err == nil {
			n.returnConnectionToPool(conn, false)
		}
	}
	n.connMtx.Unlock()

	n.expireTicker = time.NewTicker(thirtySeconds)
	go n.expireIdleConnections()

	n.setState(RUNNING)

	logDebug("[Node] (%v) started", n)

	// TODO emit stateChange event, do we care?
	return
}

func (n *Node) Stop() (err error) {
	if err = n.stateCheck(CREATED, HEALTH_CHECKING); err != nil {
		return
	}
	n.stop <- true
	n.expireTicker.Stop()
	n.setState(SHUTTING_DOWN)
	logDebug("[Node] (%v) shutting down.", n)
	n.shutdown()
	return
}

func (n *Node) Execute(cmd Command) (executed bool, err error) {
	executed = false

	if err = n.stateCheck(RUNNING, HEALTH_CHECKING); err != nil {
		return
	}

	n.stateMtx.RLock()
	defer n.stateMtx.RUnlock()
	if n.state == RUNNING {
		if conn := n.getAvailableConnection(); conn == nil {
			// TODO new conn and execute, maybe retry
		} else {
			logDebug("[Node] (%v) - executing command '%v'", n, cmd.Name())
			if err = conn.execute(cmd); err == nil {
				executed = true
				n.returnConnectionToPool(conn, true)
			} else {
				executed = false
				n.returnConnectionToPool(conn, true)
				// TODO retry command if retries remain by calling n.Execute
				// after decrementing # of tries.
			}
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
	if n.state < SHUTTING_DOWN {
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
		n.setState(ERROR)
		return
	}

	if n.currentNumConnections == 0 {
		n.setState(SHUTDOWN)
		logDebug("[Node] (%v) shut down.", n)
	} else {
		// Should never happen
		panic(fmt.Sprintf("[Node] (%v); Connections still in use.", n))
	}

	return
}

func (n *Node) setState(s state) {
	n.stateMtx.Lock()
	defer n.stateMtx.Unlock()
	n.state = s
	return
}

func (n *Node) stateCheck(allowed ...state) (err error) {
	n.stateMtx.RLock()
	defer n.stateMtx.RUnlock()
	stateChecked := false
	for _, s := range allowed {
		if n.state == s {
			stateChecked = true
			break
		}
	}
	if !stateChecked {
		err = fmt.Errorf("[Node]: Illegal State; required %s: current: %s", allowed, n.state)
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

func (n *Node) expireIdleConnections() {
	for {
		select {
		case <-n.stop:
			logDebug("[Node] (%v) idle connection expiration routine quitting!")
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
