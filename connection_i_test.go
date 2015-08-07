// +build integration

package riak

import (
	"bytes"
	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
	"github.com/golang/protobuf/proto"
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

	sawConnection := false

	go func() {
		c, err := ln.Accept()
		defer c.Close()
		if err != nil {
			t.Error(err.Error())
		}
		sawConnection = true
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
		c, err := ln.Accept()
		if err != nil {
			t.Error(err)
		}
		defer c.Close()

		var errcode uint32 = 1
		errmsg := bytes.NewBufferString("this is an error")
		rpbErr := &rpb_riak.RpbErrorResp{
			Errcode: &errcode,
			Errmsg:  errmsg.Bytes(),
		}

		encoded, err := proto.Marshal(rpbErr)
		if err != nil {
			t.Error(err)
		}

		data := buildRiakMessage(rpbCode_RpbErrorResp, encoded)
		count, err := c.Write(data)
		if err != nil {
			t.Error(err)
		}
		if count != len(data) {
			t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
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
		c, err := ln.Accept()
		defer c.Close()
		if err != nil {
			t.Error(err)
		}
		writePingResp(t, c)
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
