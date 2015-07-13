// +build integration

package riak

import (
	"net"
	"os"
	"testing"
	"time"
)

func init() {
	setLogWriter(os.Stderr)
}

func TestPing(t *testing.T) {
	var (
		addr *net.TCPAddr
		err  error
		conn *connection
	)
	addr, err = net.ResolveTCPAddr("tcp4", "riak-test:10017")
	if err != nil {
		t.Error(err.Error())
	}
	opts := &connectionOptions{
		remoteAddress:  addr,
		connectTimeout: time.Second * 5,
		requestTimeout: time.Millisecond * 500,
		healthCheck:    &PingCommand{},
	}
	if conn, err = newConnection(opts); err == nil {
		if err = conn.connect(); err == nil {
			cmd := &PingCommand{}
			if expected, actual := false, conn.inFlight; expected != actual {
				t.Errorf("expected %v, got: %v", expected, actual)
			}
			if err = conn.execute(cmd); err == nil {
				if cmd.Successful() != true {
					t.Error("ping did not return true")
				}
			}
		}
	}
	if err != nil {
		t.Error(err.Error())
	}
}
