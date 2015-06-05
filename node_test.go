package riak

import (
	"testing"
)

func TestEnsuredDefaultNodeValues(t *testing.T) {
	node, err := NewNode(nil)
	if err != nil {
		t.Error(err.Error())
	}
	if node.addr.Port != 8087 {
		t.Errorf("expected port 8087, got: %s", string(node.addr.Port))
	}
	if node.addr.Zone != "" {
		t.Errorf("expected empty zone, got: %s", string(node.addr.Zone))
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
	if node.connectTimeout != defaultConnectTimeout {
		t.Errorf("expected %v, got: %v", defaultConnectTimeout, node.connectTimeout)
	}
	if node.requestTimeout != defaultRequestTimeout {
		t.Errorf("expected %v, got: %v", defaultRequestTimeout, node.requestTimeout)
	}
	if node.healthCheck == nil {
		t.Error("expected a healthCheck Command, got nil")
	}
}
