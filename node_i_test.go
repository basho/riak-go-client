// +build integration

package riak

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestCreateNodeWithOptionsAndStart(t *testing.T) {
	remoteAddress := getRiakAddress()
	opts := &NodeOptions{
		RemoteAddress:       remoteAddress,
		MinConnections:      2,
		MaxConnections:      2048,
		IdleTimeout:         thirtyMinutes,
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
	expectedPort := getRiakPort()
	if node.addr.Port != int(expectedPort) {
		t.Errorf("expected port %d, got: %d", expectedPort, node.addr.Port)
	}
	if node.addr.Zone != "" {
		t.Errorf("expected empty zone, got: %s", string(node.addr.Zone))
	}
	if expected, actual := node.minConnections, opts.MinConnections; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := node.maxConnections, opts.MaxConnections; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := node.idleTimeout, opts.IdleTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if err := node.start(); err != nil {
		t.Error(err)
	}
	if expected, actual := node.minConnections, uint16(len(node.available)); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	for _, conn := range node.available {
		if conn == nil {
			t.Error("got unexpected nil value")
			continue
		}
		if conn.addr.Port != int(expectedPort) {
			t.Errorf("expected port %d, got: %d", expectedPort, node.addr.Port)
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
	}
	if err := node.stop(); err != nil {
		t.Error(err)
	}
}

func TestRecoverViaDefaultPingHealthCheck(t *testing.T) {
	stateChan := make(chan state, 5)
	origSetStateFunc := setStateFunc
	setStateFunc = func(s *stateData, st state) {
		origSetStateFunc(s, st)
		logDebug("[TestRecoverViaDefaultPingHealthCheck]", "sending state '%v' down stateChan", st)
		stateChan <- st
	}
	defer func() {
		setStateFunc = origSetStateFunc
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:13337")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	connects := 0

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); ok {
					return
				} else {
					t.Error(err)
					return
				}
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

	opts := &NodeOptions{
		RemoteAddress:       "127.0.0.1:13337",
		MinConnections:      0,
		HealthCheckInterval: 50 * time.Millisecond,
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err)
	}
	node.start()

	ping := &PingCommand{}
	var executed bool
	executed, err = node.execute(ping)
	if executed == false {
		t.Fatal("expected ping to be executed")
	}
	if err == nil {
		t.Fatal("expected non-nil error")
	}

	nodeState := <-stateChan
	if expected, actual := nodeCreated, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := nodeRunning, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := nodeHealthChecking, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := nodeRunning, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	logDebug("[TestRecoverViaDefaultPingHealthCheck]", "stopping node")
	node.stop()

	nodeState = <-stateChan
	if expected, actual := nodeShuttingDown, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := nodeShutdown, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	close(stateChan)
}

func TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck(t *testing.T) {
	port := 13338
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	stateChan := make(chan state)

	origSetStateFunc := setStateFunc
	defer func() {
		setStateFunc = origSetStateFunc
	}()

	var node *Node
	go func() {
		setStateFunc = func(s *stateData, st state) {
			origSetStateFunc(s, st)
			logDebug("[TestRecoverAfterConnectionComesUpViaDefaultPingHealthCheck]", "sending state '%v' down stateChan", st)
			stateChan <- st
		}
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
			if !listenerStarted && nodeState == 3 {
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
