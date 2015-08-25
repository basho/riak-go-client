// +build integration

package riak

import (
	"net"
	"reflect"
	"testing"
)

func TestNewClientWithPort(t *testing.T) {
	ports := []uint16{1234, 5678}
	for _, p := range ports {
		o := &testListenerOpts{
			test: t,
			host: "127.0.0.1",
			port: p,
		}
		tl := newTestListener(o)
		tl.start()
		defer tl.stop()
	}

	opts := &NewClientOptions{
		Port: 1234,
		RemoteAddresses: []string{
			"127.0.0.1",
			"127.0.0.1:5678",
			"127.0.0.1",
		},
	}
	c, err := NewClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	var addr *net.TCPAddr
	addr, err = net.ResolveTCPAddr("tcp", "127.0.0.1:1234")
	if err != nil {
		t.Error(err)
	}
	if expected, actual := true, reflect.DeepEqual(addr, c.cluster.nodes[0].addr); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
	addr, err = net.ResolveTCPAddr("tcp", "127.0.0.1:5678")
	if err != nil {
		t.Error(err)
	}
	if expected, actual := true, reflect.DeepEqual(addr, c.cluster.nodes[1].addr); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
	addr, err = net.ResolveTCPAddr("tcp", "127.0.0.1:1234")
	if err != nil {
		t.Error(err)
	}
	if expected, actual := true, reflect.DeepEqual(addr, c.cluster.nodes[2].addr); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
}
