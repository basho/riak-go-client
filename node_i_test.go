// +build integration

package riak

import (
	"net"
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
	if err := node.Start(); err != nil {
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
	if err := node.Stop(); err != nil {
		t.Error(err)
	}
}

func TestRecoverViaDefaultPingHealthCheck(t *testing.T) {
	stateChan := make(chan state, 4)
	origSetStateFunc := setStateFunc
	setStateFunc = func(s *stateData, st state) {
		origSetStateFunc(s, st)
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
	logDebug("[TestRecoverViaDefaultPingHealthCheck] listener started")

	connects := 0

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				break
			}
			connects++
			if connects == 1 {
				c.Close()
			} else {
				writePingResp(t, c)
				break
			}
		}
	}()

	opts := &NodeOptions{
		RemoteAddress:       "127.0.0.1:13337",
		MinConnections:      0,
		HealthCheckInterval: time.Second,
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err)
	}
	node.Start()
	defer node.Stop()
	logDebug("[TestRecoverViaDefaultPingHealthCheck] node started")

	ping := &PingCommand{}
	executed, err := node.Execute(ping)
	if executed == true {
		t.Error("expected error executing")
	}
	if err == nil {
		t.Error("expected non-nil error")
	}
	nodeState := <-stateChan
	if expected, actual := NODE_CREATED, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := NODE_RUNNING, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := NODE_HEALTH_CHECKING, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	nodeState = <-stateChan
	if expected, actual := NODE_RUNNING, nodeState; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
