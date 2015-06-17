// +build integration

package riak

import (
	"bytes"
	"net"
	"testing"
	"time"
    "github.com/golang/protobuf/proto"
)

import rpb_riak "github.com/basho-labs/riak-go-client/rpb/riak"

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
		remoteAddress: addr,
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

func TestConnectionClosed(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1337")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				break
			}
			c.Close()
		}
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
		t.Error("unexpected error in connect", err)
	} else {
		cmd := &PingCommand{}
		ln.Close()
		if err := conn.execute(cmd); err != nil {
			if operr, ok := err.(*net.OpError); ok {
				t.Log("op error", operr, operr.Op)
			} else {
				t.Error("expected to see net.OpError")
			}
		} else {
			t.Error("expected error in execute")
		}
	}
}

func TestConnectionTimeout(t *testing.T) {
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

func TestHealthCheckFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:1337")
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				t.Error(err)
				break
			}

			var errcode uint32 = 1
			errmsg := bytes.NewBufferString("this is an error")
			rpbErr := &rpb_riak.RpbErrorResp{
				Errcode: &errcode,
				Errmsg:  errmsg.Bytes(),
			}

			encoded, err := proto.Marshal(rpbErr)
			if err != nil {
				t.Error(err)
				break
			}

			data := rpbWrite(0, encoded)
			count, err := c.Write(data)
			if err != nil {
				t.Error(err)
				break
			}
			if count != len(data) {
				t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
			}
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:1337")
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
