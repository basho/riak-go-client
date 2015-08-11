// +build integration

package riak

import (
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestSuccessfulConnection(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:1337")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	connChan := make(chan bool)
	go func() {
		c, err := ln.Accept()
		defer c.Close()
		if err != nil {
			t.Log(err.Error())
		}
		connChan <- true
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1337")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress: addr,
	}

	conn, err := newConnection(opts)
	if err != nil {
		t.Error(err)
	}

	if err := conn.connect(); err != nil {
		t.Error(err)
	}

	sawConnection := <-connChan

	if err := conn.close(); err != nil {
		t.Error(err)
	}

	if !sawConnection {
		t.Error("did not connect")
	}
}

func TestConnectionClosed(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1338")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			t.Error(err)
		}
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1338")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress: addr,
	}

	conn, err := newConnection(opts)
	if err != nil {
		t.Error(err)
	}

	if err := conn.connect(); err != nil {
		t.Error("unexpected error in connect", err)
	} else {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
		cmd := &PingCommand{}
		if err := conn.execute(cmd); err != nil {
			if operr, ok := err.(*net.OpError); ok {
				t.Log("op error", operr, operr.Op)
			} else if err == io.EOF {
				t.Log("saw EOF")
			} else {
				t.Errorf("expected to see net.OpError or io.EOF, but got '%s' (type: %v)", err.Error(), reflect.TypeOf(err))
			}
		} else {
			t.Error("expected error in execute")
		}
	}
}

func TestConnectionTimeout(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp4", "10.255.255.1:65535")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress:  addr,
		connectTimeout: time.Millisecond * 150,
	}

	if conn, err := newConnection(opts); err == nil {
		if err := conn.connect(); err == nil {
			t.Error("expected to see timeout error")
		} else {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("timeout error", neterr)
			} else if operr, ok := err.(*net.OpError); ok {
				t.Log("op error", operr)
			} else {
				t.Errorf("expected to see timeout error, but got '%s' (type: %v)", err.Error(), reflect.TypeOf(err))
			}
		}
	} else {
		t.Error(err)
	}
}

func TestHealthCheckFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1339")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); !ok {
					t.Error(err)
				}
				return
			}
			go handleClientMessageWithRiakError(t, conn, nil)
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1339")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress:  addr,
		connectTimeout: thirtySeconds,
		healthCheck:    &PingCommand{},
	}

	if conn, err := newConnection(opts); err == nil {
		if err := conn.connect(); err == nil {
			t.Error("expected to see error")
		} else {
			if riakError, ok := err.(Error); ok == true {
				if expected, actual := "riak error - errcode '1', errmsg 'this is an error'", riakError.Error(); expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			} else {
				t.Error("expected to see Riak error, got:", err)
			}
		}
	} else {
		t.Error(err)
	}
}

func TestHealthCheckSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1340")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); !ok {
					t.Error(err)
				}
				return
			}
			go readWritePingResp(t, conn)
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1340")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress:  addr,
		connectTimeout: thirtySeconds,
		healthCheck:    &PingCommand{},
	}

	if conn, err := newConnection(opts); err == nil {
		if err := conn.connect(); err != nil {
			t.Error("unexpected error:", err)
		}
	} else {
		t.Error("unexpected error:", err)
	}
}
