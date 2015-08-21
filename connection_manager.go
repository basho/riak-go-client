package riak

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Constants identifying connectionManager state
const (
	cmCreated state = iota
	cmRunning
	cmShuttingDown
	cmShutdown
	cmError
)

type connectionManagerOptions struct {
	addr           *net.TCPAddr
	stopChan       chan bool
	minConnections uint16
	maxConnections uint16
	idleTimeout    time.Duration
	connectTimeout time.Duration
	requestTimeout time.Duration
	authOptions    *AuthOptions
}

type connectionManager struct {
	addr            *net.TCPAddr
	minConnections  uint16
	maxConnections  uint16
	idleTimeout     time.Duration
	connectTimeout  time.Duration
	requestTimeout  time.Duration
	authOptions     *AuthOptions
	stopChan        chan bool
	q               *queue
	expireTicker    *time.Ticker
	connectionCount uint16
	sync.RWMutex
	stateData
}

var (
	ErrConnectionManagerRequiresOptions     = newClientError("[connectionManager] new manager requires options")
	ErrConnectionManagerRequiresAddress     = newClientError("[connectionManager] new manager requires non-nil address")
	ErrConnectionManagerRequiresStopChannel = newClientError("[connectionManager] new manager requires non-nil stop channel")
	ErrConnMgrAllConnectionsInUse           = newClientError("[connectionManager] all connections in use at max connections reached")
)

func newConnectionManager(options *connectionManagerOptions) (*connectionManager, error) {
	if options == nil {
		return nil, ErrConnectionManagerRequiresOptions
	}
	if options.addr == nil {
		return nil, ErrConnectionManagerRequiresAddress
	}
	if options.stopChan == nil {
		return nil, ErrConnectionManagerRequiresStopChannel
	}
	if options.minConnections == 0 {
		options.minConnections = defaultMinConnections
	}
	if options.maxConnections == 0 {
		options.maxConnections = defaultMaxConnections
	}
	if options.idleTimeout == 0 {
		options.idleTimeout = defaultIdleTimeout
	}
	if options.connectTimeout == 0 {
		options.connectTimeout = defaultConnectTimeout
	}
	if options.requestTimeout == 0 {
		options.requestTimeout = defaultRequestTimeout
	}
	cm := &connectionManager{
		addr:           options.addr,
		minConnections: options.minConnections,
		maxConnections: options.maxConnections,
		idleTimeout:    options.idleTimeout,
		connectTimeout: options.connectTimeout,
		requestTimeout: options.requestTimeout,
		authOptions:    options.authOptions,
		stopChan:       options.stopChan,
		q:              newQueue(options.maxConnections),
	}
	cm.initStateData("connMgrError", "connMgrCreated", "connMgrRunning", "connMgrShuttingDown", "connMgrShutdown")
	cm.setState(cmCreated)
	return cm, nil
}

func (cm *connectionManager) String() string {
	return fmt.Sprintf("%v|%d", cm.addr, cm.count())
}

func (cm *connectionManager) start() error {
	if err := cm.stateCheck(cmCreated); err != nil {
		return err
	}
	for i := uint16(0); i < cm.minConnections; i++ {
		conn, err := cm.create(nil)
		if err == nil {
			cm.put(conn)
		} else {
			logErr("[connectionManager]", err)
		}
	}
	cm.expireTicker = time.NewTicker(fiveSeconds)
	go cm.expireConnections()
	cm.setState(cmRunning)
	return nil
}

func (cm *connectionManager) stop() error {
	if err := cm.stateCheck(cmRunning); err != nil {
		return err
	}

	logDebug("[connectionManager]", "shutting down")

	cm.setState(cmShuttingDown)
	cm.expireTicker.Stop()

	if cm.count() != cm.q.count() {
		logError("[connectionManager]", "stop: current connection count '%d' does NOT equal q count '%d'", cm.count(), cm.q.count())
	}

	cm.Lock()
	defer cm.Unlock()

	var f = func(v interface{}) (bool, bool) {
		if v == nil {
			return true, false
		}
		conn := v.(*connection)
		if err := conn.close(); err != nil {
			logErr("[connectionManager] error when closing connection in stop()", err)
		}
		cm.connectionCount--
		if cm.connectionCount == 0 {
			return true, false
		} else {
			return false, false
		}
	}
	err := cm.q.iterate(f)
	cm.q.destroy()

	if err == nil {
		cm.setState(cmShutdown)
	} else {
		cm.setState(cmError)
	}

	return err
}

func (cm *connectionManager) count() uint16 {
	cm.RLock()
	defer cm.RUnlock()
	return cm.connectionCount
}

func (cm *connectionManager) create(hc Command) (*connection, error) {
	if !cm.isStateLessThan(cmShuttingDown) {
		return nil, nil
	}
	cm.Lock()
	defer cm.Unlock()
	if cm.connectionCount < cm.maxConnections {
		opts := &connectionOptions{
			remoteAddress:  cm.addr,
			connectTimeout: cm.connectTimeout,
			requestTimeout: cm.requestTimeout,
			authOptions:    cm.authOptions,
			healthCheck:    hc,
		}
		conn, err := newConnection(opts)
		if err != nil {
			return nil, err
		}
		err = conn.connect()
		if err != nil {
			return nil, err
		}
		cm.connectionCount++
		return conn, nil
	} else {
		return nil, ErrConnMgrAllConnectionsInUse
	}
}

func (cm *connectionManager) get() (*connection, error) {
	var conn *connection
	currentConnCount := cm.count()
	c := uint16(0)
	var f = func(v interface{}) (bool, bool) {
		if v == nil {
			// connection pool is empty
			return true, false
		}
		c++
		conn = v.(*connection)
		if conn.available() {
			// we found our connection, don't re-queue
			return true, false
		} else {
			if c == currentConnCount {
				// stop searching and re-queue conn
				return true, true
			} else {
				// keep going and re-queue conn
				return false, true
			}
		}
	}
	err := cm.q.iterate(f)
	if err != nil {
		return nil, err
	}

	if conn != nil {
		return conn, nil
	}

	// NB: if we get here, there were no available connections
	return cm.create(nil)
}

func (cm *connectionManager) put(conn *connection) error {
	if cm.isStateLessThan(cmShuttingDown) {
		return cm.q.enqueue(conn)
	} else {
		// shutting down
		logDebug("[connectionManager]", "(%v)|Connection returned during shutdown.", cm)
		cm.Lock()
		defer cm.Unlock()
		cm.connectionCount--
		conn.close() // NB: discard error
	}
	return nil
}

func (cm *connectionManager) remove(conn *connection) error {
	if cm.isStateLessThan(cmShuttingDown) {
		cm.Lock()
		defer cm.Unlock()
		cm.connectionCount--
		return conn.close()
	}
	return nil
}

func (cm *connectionManager) expireConnections() {
	logDebug("[connectionManager]", "connection expiration routine is starting")
	for {
		select {
		case <-cm.stopChan:
			logDebug("[connectionManager]", "connection expiration routine is quitting")
			return
		case t := <-cm.expireTicker.C:
			if !cm.isStateLessThan(cmShuttingDown) {
				logDebug("[connectionManager]", "(%v) connection expiration routine is quitting.", cm)
			}

			logDebug("[connectionManager]", "(%v) expiring connections at %v", cm, t)

			currentConnCount := cm.count()
			c := uint16(0)
			expiredCount := uint16(0)
			now := time.Now()

			var f = func(v interface{}) (bool, bool) {
				if v == nil {
					// connection pool is empty
					return true, false
				}
				c++
				if !cm.isStateLessThan(cmShuttingDown) {
					return true, true
				}
				conn := v.(*connection)
				// expire connection if not available or if it has passed idle timeout
				if !conn.available() || (now.Sub(conn.lastUsed) >= cm.idleTimeout) {
					cm.Lock()
					cm.connectionCount--
					cm.Unlock()
					if err := conn.close(); err != nil {
						logErr("[connectionManager]", err)
					}
					expiredCount++
					return false, false
				}
				if c == currentConnCount {
					return true, true
				} else {
					return false, true
				}
			}

			if err := cm.q.iterate(f); err != nil {
				logErr("[connectionManager]", err)
			}

			logDebug("[connectionManager]", "(%v) expired %d connections.", cm, expiredCount)

			if !cm.isStateLessThan(cmShuttingDown) {
				logDebug("[connectionManager]", "(%v) connection expiration routine is quitting.", cm)
			}
		}
	}
}
