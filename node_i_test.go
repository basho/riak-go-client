// +build integration

package riak

import (
	"net"
	"testing"
	"time"
)

func TestCreateNodeWithOptionsAndStart(t *testing.T) {
	o := &testListenerOpts{
		test: t,
		host: "127.0.0.1",
		port: 13340,
	}
	tl := newTestListener(o)
	tl.start()
	defer tl.stop()

	count := uint16(16)
	opts := &NodeOptions{
		RemoteAddress:       tl.addr,
		MinConnections:      count,
		MaxConnections:      count,
		IdleTimeout:         thirtySeconds,
		ConnectTimeout:      thirtySeconds,
		RequestTimeout:      thirtySeconds,
		HealthCheckInterval: time.Millisecond * 500,
		HealthCheckBuilder:  &PingCommandBuilder{},
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err.Error())
	}
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.addr.Port != int(tl.port) {
		t.Errorf("expected port %d, got: %d", tl.port, node.addr.Port)
	}
	if node.addr.Zone != "" {
		t.Errorf("expected empty zone, got: %s", string(node.addr.Zone))
	}
	if expected, actual := opts.MinConnections, node.cm.minConnections; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := opts.MaxConnections, node.cm.maxConnections; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := opts.IdleTimeout, node.cm.idleTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if err := node.start(); err != nil {
		t.Error(err)
	}
	var f = func(v interface{}) (bool, bool) {
		conn := v.(*connection)
		if conn == nil {
			t.Error("got unexpected nil value")
			return true, false
		}
		if expected, actual := int(tl.port), conn.addr.Port; expected != actual {
			t.Errorf("expected %d, got: %d", expected, actual)
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %s", string(conn.addr.Zone))
		}
		if conn.healthCheck != nil {
			t.Error("expected nil conn.healthCheck")
		}
		if expected, actual := conn.connectTimeout, opts.ConnectTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		if expected, actual := conn.requestTimeout, opts.RequestTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		return false, true
	}
	if err := node.cm.q.iterate(f); err != nil {
		t.Error(err)
	}
	if err := node.stop(); err != nil {
		t.Error(err)
	}
}

func TestRecoverViaDefaultPingHealthCheck(t *testing.T) {
	connects := 0
	var onConn = func(c net.Conn) bool {
		connects++
		if connects == 1 {
			c.Close()
		} else {
			readWritePingResp(t, c, true)
		}
		return true
	}
	o := &testListenerOpts{
		test:   t,
		host:   "127.0.0.1",
		port:   13337,
		onConn: onConn,
	}
	tl := newTestListener(o)
	tl.start()
	defer tl.stop()

	doneChan := make(chan struct{})
	stateChan := make(chan state)

	go func() {
		opts := &NodeOptions{
			RemoteAddress:       tl.addr,
			MinConnections:      0,
			HealthCheckInterval: 50 * time.Millisecond,
		}
		node, err := NewNode(opts)
		if err != nil {
			t.Error(err)
		}

		origSetStateFunc := node.setStateFunc
		node.setStateFunc = func(sd *stateData, st state) {
			origSetStateFunc(&node.stateData, st)
			logDebug("[TestRecoverViaDefaultPingHealthCheck]", "sending state '%v' down stateChan", st)
			stateChan <- st
		}

		node.start()
		ping := &PingCommand{}
		executed, err := node.execute(ping)
		if executed == false {
			t.Fatal("expected ping to be executed")
		}
		if err == nil {
			t.Fatal("expected non-nil error")
		}
		logDebug("[TestRecoverViaDefaultPingHealthCheck]", "waiting to stop node")
		<-doneChan
		logDebug("[TestRecoverViaDefaultPingHealthCheck]", "stopping node")
		node.stop()
	}()

	checkStatesFunc := func(states []state) {
		for i := 0; i < len(states); i++ {
			nodeState := <-stateChan
			if expected, actual := states[i], nodeState; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			} else {
				logDebug("[TestRecoverViaDefaultPingHealthCheck]", "saw state %d", nodeState)
			}
		}
	}

	expectedStates := []state{
		nodeRunning, nodeHealthChecking, nodeRunning,
	}

	checkStatesFunc(expectedStates)

	close(doneChan)

	expectedStates = []state{
		nodeShuttingDown, nodeShutdown,
	}

	checkStatesFunc(expectedStates)

	close(stateChan)
}

func TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck(t *testing.T) {
	o := &testListenerOpts{
		test: t,
		host: "127.0.0.1",
		port: 13338,
	}
	tl := newTestListener(o)
	defer tl.stop()

	stateChan := make(chan state)
	recoveredChan := make(chan struct{})

	var node *Node
	opts := &NodeOptions{
		ConnectTimeout: 125 * time.Millisecond,
		RemoteAddress:  tl.addr,
	}
	var err error
	node, err = NewNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	origSetStateFunc := node.setStateFunc

	go func() {
		node.setStateFunc = func(sd *stateData, st state) {
			origSetStateFunc(&node.stateData, st)
			logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "sending state '%v' down stateChan", st)
			stateChan <- st
		}
		node.start()

		pc := &PingCommand{}
		node.execute(pc)
		for {
			select {
			case <-recoveredChan:
				break
			case <-time.After(time.Second):
				logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "waiting for recovery...")
				pc := &PingCommand{}
				node.execute(pc)
			}
		}
	}()

	go func() {
		listenerStarted := false
		nodeIsRunningCount := 0
		for {
			if nodeState, ok := <-stateChan; ok {
				logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "nodeState: '%v'", nodeState)
				if node.isCurrentState(nodeRunning) {
					nodeIsRunningCount++
				}
				if nodeIsRunningCount == 2 {
					// This is the second time node has entered nodeRunning state, so it must have recovered via the health check
					logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "SUCCESS node recovered via health check")
					close(recoveredChan)
					break
				}
				if !listenerStarted && node.isCurrentState(nodeHealthChecking) {
					logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "STARTING LISTENER")
					tl.start()
				}
			} else {
				t.Error("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck] stateChan closed before recovering via health check")
				break
			}
		}
	}()

	select {
	case <-recoveredChan:
		logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "recovered")
		node.setStateFunc = origSetStateFunc
		node.stop()
		close(stateChan)
	case <-time.After(5 * time.Second):
		t.Error("test timed out")
	}
}
