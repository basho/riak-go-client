// +build integration

package riak

import (
	"testing"
)

func TestCreateNodeWithOptionsAndStart(t *testing.T) {
	opts := &NodeOptions{
		RemoteAddress:      "riak-test:10017",
		MinConnections:     2,
		MaxConnections:     2048,
		IdleTimeout:        thirtyMinutes,
		ConnectTimeout:     thirtySeconds,
		RequestTimeout:     thirtySeconds,
		HealthCheckBuilder: &PingCommandBuilder{},
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err.Error())
	}
	if node.addr.Port != 10017 {
		t.Errorf("expected port 10017, got: %s", string(node.addr.Port))
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
	var lastAddr *Command
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
		if conn.addr.Port != 10017 {
			t.Errorf("expected port 10017, got: %s", string(conn.addr.Port))
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %s", string(conn.addr.Zone))
		}
		if conn.healthCheck == nil {
			t.Error("expected non-nil conn.healthCheck")
		} else {
			currentAddr := &conn.healthCheck
			if lastAddr == currentAddr {
				t.Errorf("expected unique conn.healthCheck struct lastAddr %v, currentAddr %v", lastAddr, currentAddr)
			} else {
				lastAddr = currentAddr
			}
		}
		if expected, actual := conn.connectTimeout, opts.ConnectTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		if expected, actual := conn.requestTimeout, opts.RequestTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
	}
}
