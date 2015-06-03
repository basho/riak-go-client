package riak

import (
	"bytes"
	"strings"
	"testing"
)

func TestDebugLogging(t *testing.T) {
	buf := new(bytes.Buffer)
	setLogWriter(buf)
	logDebug("[TestDebugLogging] test: %s", "frazzle")
	logged := buf.String()
	if !strings.Contains(logged, "test: frazzle") {
		t.Errorf("expected frazzle, got: %s", logged)
	}
}
