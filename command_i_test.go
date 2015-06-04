// +build integration

package riak

import (
	"os"
	"testing"
	"time"
)

func init() {
	setLogWriter(os.Stderr)
}

func TestPing(t *testing.T) {
	var (
		err  error
		conn *connection
	)
	opts := &connectionOptions{
		remoteAddress:     "riak-test:10017",
		connectionTimeout: time.Second * 5,
		requestTimeout:    time.Millisecond * 500,
		maxBufferSize:     1024,
		initBufferSize:    1024,
	}
	if conn, err = newConnection(opts); err == nil {
		if err = conn.connect(); err == nil {
			cmd := &PingCommand{}
			if err = conn.execute(cmd); err == nil {
				if cmd.Result != true {
					t.Error("ping did not return true")
				}
			}
		}
	}
	if err != nil {
		t.Error(err.Error())
	}
}
