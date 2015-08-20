package riak

import (
	"testing"
)

func TestCreateConnectionManager(t *testing.T) {
	_, err := newConnectionManager(nil)
	if err == nil {
		t.Error("expected non-nil error when creating without options")
	}
}
