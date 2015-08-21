// +build integration

package riak

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestCreateNodeWithOptionsAndStart(t *testing.T) {
	port := 13340
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); !ok {
					t.Error(err)
				}
				return
			}
			go func() {
				for {
					if !readWritePingResp(t, c, false) {
						break
					}
				}
			}()
		}
	}()

	count := uint16(16)
	opts := &NodeOptions{
		RemoteAddress:       addr,
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
	if node.addr.Port != int(port) {
		t.Errorf("expected port %d, got: %d", port, node.addr.Port)
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
	c := uint16(0)
	var f = func(v interface{}) (bool, bool) {
		c++
		conn := v.(*connection)
		if conn == nil {
			t.Error("got unexpected nil value")
			return true, false
		}
		if conn.addr.Port != port {
			t.Errorf("expected port %d, got: %d", port, node.addr.Port)
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
		if c == count {
			return true, true
		} else {
			return false, true
		}
	}
	if err := node.cm.q.iterate(f); err != nil {
		t.Error(err)
	}
	if err := node.stop(); err != nil {
		t.Error(err)
	}
}

func TestRecoverViaDefaultPingHealthCheck(t *testing.T) {
	port := 13337
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	doneChan := make(chan struct{})
	stateChan := make(chan state)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	connects := 0
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); !ok {
					t.Error(err)
				}
				return
			}
			connects++
			if connects == 1 {
				c.Close()
			} else {
				readWritePingResp(t, c, true)
				return
			}
		}
	}()

	go func() {
		opts := &NodeOptions{
			RemoteAddress:       addr,
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
	port := 13338
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	stateChan := make(chan state)

	var node *Node
	go func() {
		opts := &NodeOptions{
			RemoteAddress:       addr,
			MinConnections:      0,
			HealthCheckInterval: 250 * time.Millisecond,
		}
		var err error
		node, err = NewNode(opts)
		if err != nil {
			t.Fatal(err)
		}

		origSetStateFunc := node.setStateFunc
		node.setStateFunc = func(sd *stateData, st state) {
			origSetStateFunc(&node.stateData, st)
			logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "sending state '%v' down stateChan", st)
			stateChan <- st
		}

		node.start()
		nodeIsRunningCount := 0
		for i := 0; i < 10; i++ {
			if node.isCurrentState(nodeRunning) {
				nodeIsRunningCount++
			}
			if nodeIsRunningCount == 2 {
				break
			}
			ping := &PingCommand{}
			_, err = node.execute(ping)
			if err != nil {
				t.Log(err)
			}
			time.Sleep(time.Second)
		}
		node.stop()
		close(stateChan)
	}()

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
				break
			}
			if !listenerStarted && node.isCurrentState(nodeHealthChecking) {
				listenerStarted = true
				ln, err := net.Listen("tcp", addr)
				if err != nil {
					t.Error(err)
				}
				defer ln.Close()

				go func() {
					for {
						c, err := ln.Accept()
						if err != nil {
							if _, ok := err.(*net.OpError); !ok {
								t.Error(err)
							}
							return
						}
						go func() {
							for {
								if !readWritePingResp(t, c, false) {
									break
								}
							}
						}()
					}
				}()
			}
		} else {
			t.Error("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck] stateChan closed before recovering via health check")
			break
		}
	}
}
