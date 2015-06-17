// +build integration

package riak

import (
	"net"
	"testing"
	"time"
)

func TestSuccessfulConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1337")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	sawConnection := false

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				break
			}
			sawConnection = true
			c.Close()
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1337")
	if err != nil {
		t.Error(err.Error())
	}

	opts := &connectionOptions{
		remoteAddress:  addr,
		connectTimeout: thirtySeconds,
		requestTimeout: thirtyMinutes,
	}

	conn, err := newConnection(opts)
	if err != nil {
		t.Error(err)
	}

	if err := conn.connect(); err != nil {
		t.Error(err)
	}

	if err := conn.close(); err != nil {
		t.Error(err)
	}

	if !sawConnection {
		t.Error("did not connect")
	}
}

func TestFailedConnection(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:65535")
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
			} else {
				t.Error("expected to see timeout error")
			}
		}
	} else {
		t.Error(err)
	}
}
