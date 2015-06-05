package riak

import (
	"net"
	"testing"
)

func TestCreateNodeWithOptions(t *testing.T) {
	opts := &NodeOptions{
		RemoteAddress:  "8.8.8.8:1234",
		MinConnections: 2,
		MaxConnections: 2048,
		IdleTimeout:    thirtyMinutes,
		ConnectTimeout: thirtySeconds,
		RequestTimeout: thirtySeconds,
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err.Error())
	}
	if node.addr.Port != 1234 {
		t.Errorf("expected port 1234, got: %s", string(node.addr.Port))
	}
	if node.addr.Zone != "" {
		t.Errorf("expected empty zone, got: %s", string(node.addr.Zone))
	}
	var testIP = net.ParseIP("8.8.8.8")
	if !node.addr.IP.Equal(testIP) {
		t.Errorf("expected %v, got: %v", testIP, node.addr.IP)
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
	if expected, actual := node.minConnections, uint16(len(node.available)); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	for _, conn := range node.available {
		if conn == nil {
			t.Error("got unexpected nil value")
		}
		if conn.addr.Port != 1234 {
			t.Errorf("expected port 1234, got: %s", string(conn.addr.Port))
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %s", string(conn.addr.Zone))
		}
		if !conn.addr.IP.Equal(testIP) {
			t.Errorf("expected %v, got: %v", testIP, conn.addr.IP)
		}
		if expected, actual := conn.connectTimeout, opts.ConnectTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		if expected, actual := conn.requestTimeout, opts.RequestTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
	}
}

func TestEnsureDefaultNodeValues(t *testing.T) {
	node, err := NewNode(nil)
	if err != nil {
		t.Error(err.Error())
	}
	if node.addr.Port != 8087 {
		t.Errorf("expected port 8087, got: %v", string(node.addr.Port))
	}
	if node.addr.Zone != "" {
		t.Errorf("expected empty zone, got: %v", string(node.addr.Zone))
	}
	if !node.addr.IP.Equal(localhost) {
		t.Errorf("expected %v, got: %v", localhost, node.addr.IP)
	}
	if node.minConnections != defaultMinConnections {
		t.Errorf("expected %v, got: %v", defaultMinConnections, node.minConnections)
	}
	if node.maxConnections != defaultMaxConnections {
		t.Errorf("expected %v, got: %v", defaultMaxConnections, node.maxConnections)
	}
	if node.idleTimeout != defaultIdleTimeout {
		t.Errorf("expected %v, got: %v", defaultIdleTimeout, node.idleTimeout)
	}
	if expected, actual := node.minConnections, uint16(len(node.available)); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	for _, conn := range node.available {
		if conn == nil {
			t.Error("got unexpected nil value")
		}
		if conn.addr.Port != 8087 {
			t.Errorf("expected port 8087, got: %v", string(conn.addr.Port))
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %v", string(conn.addr.Zone))
		}
		if !conn.addr.IP.Equal(localhost) {
			t.Errorf("expected %v, got: %v", localhost, conn.addr.IP)
		}
		if expected, actual := conn.connectTimeout, defaultConnectTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		if expected, actual := conn.requestTimeout, defaultRequestTimeout; expected != actual {
			t.Errorf("expected %v, got: %v", expected, actual)
		}
		if conn.healthCheck == nil {
			t.Error("expected a conn.healthCheck Command, got nil")
		}
	}
}
