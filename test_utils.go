package riak

import (
	"net"
	"testing"
)

func writePingResp(t *testing.T, c net.Conn) (success bool) {
	success = false
	data := buildRiakMessage(rpbCode_RpbPingResp, nil)
	count, err := c.Write(data)
	if err != nil {
		t.Error(err)
	}
	if count != len(data) {
		t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
	}
	success = true
	return
}
