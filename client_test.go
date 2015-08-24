package riak

import (
	"strings"
	"testing"
)

func TestSplitRemoteAddress(t *testing.T) {
	s := strings.SplitN(defaultRemoteAddress, ":", 2)
	if expected, actual := "127.0.0.1", s[0]; expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
	if expected, actual := "8087", s[1]; expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
}

func TestNewClientWithInvalidData(t *testing.T) {
	opts := &NewClientOptions{
		RemoteAddresses: []string{
			"FOO:BAR:BAZ",
		},
	}
	c, err := NewClient(opts)
	if err == nil {
		t.Errorf("expected non-nil error, %v", c)
	}

	opts = &NewClientOptions{
		RemoteAddresses: []string{
			"127.0.0.1:FRAZZLE",
		},
	}
	c, err = NewClient(opts)
	if err == nil {
		t.Errorf("expected non-nil error, %v", c)
	}
}
