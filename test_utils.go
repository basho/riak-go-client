package riak

import (
	"encoding/json"
	"net"
	"testing"
	"time"
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

func jsonDump(val interface{}) {
	if val == nil {
		logDebug("[jsonDump] NIL VAL")
	} else {
		if json, err := json.MarshalIndent(val, "", "  "); err != nil {
			logDebug("[jsonDump] %s", err.Error())
		} else {
			logDebug("[jsonDump] %s", string(json))
		}
	}
}

func validateTimeout(t *testing.T, e time.Duration, a uint32) {
	actualDuration := time.Duration(a) * time.Millisecond
	if expected, actual := e, actualDuration; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
