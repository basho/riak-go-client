package riak

import (
	"net"
	"testing"
	"time"
)

const (
	thirtySeconds = time.Second * 30
	thirtyMinutes = time.Minute * 30
)

func TestCreateConnection(t *testing.T) {
	opts := &connectionOptions{
		remoteAddress:     "127.0.0.1:8098",
		connectionTimeout: thirtySeconds,
		requestTimeout:    thirtyMinutes,
		maxBufferSize:     1024,
		initBufferSize:    1024,
	}
	if conn, err := newConnection(opts); err == nil {
		if conn.addr.Port != 8098 {
			t.Errorf("expected port 8098, got: %s", string(conn.addr.Port))
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %s", string(conn.addr.Zone))
		}
		localhost := net.ParseIP("127.0.0.1")
		if !conn.addr.IP.Equal(localhost) {
			t.Errorf("expected %v, got: %v", localhost, conn.addr.IP)
		}
		if conn.connectionTimeout != thirtySeconds {
			t.Errorf("expected %v, got: %v", thirtySeconds, conn.connectionTimeout)
		}
		if conn.requestTimeout != thirtyMinutes {
			t.Errorf("expected %v, got: %v", thirtyMinutes, conn.requestTimeout)
		}
		if conn.maxBufferSize != 1024 {
			t.Errorf("expected 1024, got: %v", conn.maxBufferSize)
		}
		if conn.initBufferSize != 1024 {
			t.Errorf("expected 1024, got: %v", conn.initBufferSize)
		}
	} else {
		t.Error(err.Error())
	}
}

func TestCreateConnectionWithBadAddress(t *testing.T) {
	opts := &connectionOptions{remoteAddress: "123456.89.9813948.19328419348:80983r6"}
	if _, err := newConnection(opts); err == nil {
		t.Error("expected error")
	} else {
		t.Log(err)
	}
}

func TestCreateConnectionRequiresOptions(t *testing.T) {
	if _, err := newConnection(nil); err == nil {
		t.Error("expected error when creating Connection without options")
	}
}

func TestCreateConnectionRequiresAddress(t *testing.T) {
	opts := &connectionOptions{}
	if _, err := newConnection(opts); err == nil {
		t.Error("expected error when creating Connection without address")
	}
}

func TestEnsureDefaultConnectionValues(t *testing.T) {
	opts := &connectionOptions{remoteAddress: "127.0.0.1:8098"}
	if conn, err := newConnection(opts); err == nil {
		if conn.connectionTimeout != defaultConnectionTimeout {
			t.Errorf("expected %v, got: %v", defaultConnectionTimeout, conn.connectionTimeout)
		}
		if conn.requestTimeout != defaultRequestTimeout {
			t.Errorf("expected %v, got: %v", defaultRequestTimeout, conn.requestTimeout)
		}
		if conn.maxBufferSize != defaultMaxBuffer {
			t.Errorf("expected %v, got: %v", defaultMaxBuffer, conn.maxBufferSize)
		}
		if conn.initBufferSize != defaultInitBuffer {
			t.Errorf("expected %v, got: %v", defaultInitBuffer, conn.initBufferSize)
		}
	} else {
		t.Error(err.Error())
	}
}
