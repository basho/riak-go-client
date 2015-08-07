package riak

import (
	"net"
	"testing"
)

func TestCreateNodeWithOptions(t *testing.T) {
	builder := &PingCommandBuilder{}
	opts := &NodeOptions{
		RemoteAddress:       "8.8.8.8:1234",
		MinConnections:      2,
		MaxConnections:      2048,
		IdleTimeout:         thirtyMinutes,
		ConnectTimeout:      thirtySeconds,
		RequestTimeout:      thirtySeconds,
		HealthCheckInterval: thirtySeconds,
		HealthCheckBuilder:  builder,
	}
	node, err := NewNode(opts)
	if err != nil {
		t.Error(err.Error())
	}
	if expected, actual := "8.8.8.8:1234|0", node.String(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := nodeCreated, node.getState(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
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
	if expected, actual := node.connectTimeout, opts.ConnectTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := node.requestTimeout, opts.RequestTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := 0, len(node.available); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := node.healthCheckInterval, opts.HealthCheckInterval; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := builder, node.healthCheckBuilder; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
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
	if expected, actual := defaultConnectTimeout, node.connectTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := defaultRequestTimeout, node.requestTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := defaultConnectTimeout, node.connectTimeout; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := 0, len(node.available); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if expected, actual := defaultHealthCheckInterval, node.healthCheckInterval; expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
	if node.healthCheckBuilder != nil {
		t.Errorf("expected nil, got: %v", node.healthCheckBuilder)
	}
	if expected, actual := nodeCreated, node.getState(); expected != actual {
		t.Errorf("expected %v, got: %v", expected, actual)
	}
}
