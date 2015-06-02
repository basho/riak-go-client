package riak

import (
	"net"
	"testing"
	"time"
)

const (
	thirtySeconds = time.Second * 30
	thirtyMinutes = time.Minute * 30
)

func TestCreateConnection(t *testing.T) {
	opts := &ConnectionOptions{
		RemoteAddress: "127.0.0.1:8098",
		ConnectionTimeout: thirtySeconds,
		RequestTimeout: thirtyMinutes,
		MaxBufferSize: 1024,
		InitBufferSize: 1024,
	}
	if conn, err := NewConnection(opts); err == nil {
		if conn.addr.Port != 8098 {
			t.Errorf("expected port 8098, got: %s", string(conn.addr.Port))
		}
		if conn.addr.Zone != "" {
			t.Errorf("expected empty zone, got: %s", string(conn.addr.Zone))
		}
		localhost := net.ParseIP("127.0.0.1")
		if !conn.addr.IP.Equal(localhost)  {
			t.Errorf("expected %v, got: %v", localhost, conn.addr.IP)
		}
	} else {
		t.Error(err.Error())
	}
}

func TestCreateConnectionWithBadAddress(t *testing.T) {
	opts := &ConnectionOptions{RemoteAddress: "123456.89.9813948.19328419348:80983r6"}
	if _, err := NewConnection(opts); err == nil {
		t.Error("expected error")
	}
}

/*
func TestSessionMultiple(t *testing.T) {
	client := dial()
	defer client.Close()
	s1 := client.Session()
	defer s1.Close()
	s2 := client.Session()
	defer s2.Close()

	s1name := "session-1"
	s2name := "session-2"

	if _, err := s1.SetClientId([]byte(s1name)); err != nil {
		t.Error(err.Error())
	}
	if _, err := s2.SetClientId([]byte(s2name)); err != nil {
		t.Error(err.Error())
	}

	out1, err := s1.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	if string(out1.GetClientId()) != s1name {
		t.Errorf("expected: %s, got: %s", s1name, string(out1.GetClientId()))
	}

	out2, err := s2.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	if string(out2.GetClientId()) != s2name {
		t.Errorf("expected: %s, got: %s", s2name, string(out2.GetClientId()))
	}
}
*/
