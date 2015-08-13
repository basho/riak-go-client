// +build integration

package riak

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestExecuteCommandOnCluster(t *testing.T) {
	nodeOpts := &NodeOptions{
		RemoteAddress: getRiakAddress(),
	}

	var node *Node
	var err error
	if node, err = NewNode(nodeOpts); err != nil {
		t.Error(err.Error())
	}
	if node == nil {
		t.FailNow()
	}

	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes:             nodes,
		ExecutionAttempts: 3,
	}

	if expected, actual := 1, len(opts.Nodes); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := node, opts.Nodes[0]; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	cluster, err := NewCluster(opts)
	if err != nil {
		t.Error(err.Error())
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	if expected, actual := node, cluster.nodes[0]; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if err := cluster.Start(); err != nil {
		t.Error(err.Error())
	}

	command := &PingCommand{}
	if err := cluster.Execute(command); err != nil {
		t.Error(err.Error())
	}

	if expected, actual := true, command.hasRemainingTries(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := byte(3), command.remainingTries; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := true, command.Successful(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestExecuteConcurrentCommandsOnCluster(t *testing.T) {
	nodeOpts := &NodeOptions{
		MinConnections: 32,
		MaxConnections: 64,
		RemoteAddress:  getRiakAddress(),
	}

	var node *Node
	var err error
	if node, err = NewNode(nodeOpts); err != nil {
		t.Error(err.Error())
	}
	if node == nil {
		t.FailNow()
	}

	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := NewCluster(opts)
	if err != nil {
		t.Error(err.Error())
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	if err := cluster.Start(); err != nil {
		t.Error(err.Error())
	}

	count := 32
	pingChan := make(chan *PingCommand, count)
	for i := 0; i < count; i++ {
		logDebug("[TestExecuteConcurrentCommandsOnCluster]", "i: %d", i)
		go func() {
			command := &PingCommand{}
			if err := cluster.Execute(command); err != nil {
				t.Error(err.Error())
			}
			pingChan <- command
		}()
	}

	j := 0
	for i := 0; i < count; i++ {
		select {
		case pingCommand := <-pingChan:
			logDebug("[TestExecuteConcurrentCommandsOnCluster]", "j: %d, pingCommand: %v", j, pingCommand)
			j++
		}
	}
	if expected, actual := count, j; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestExecuteCommandThreeTimesOnDifferentNodes(t *testing.T) {
	nodeCount := 3
	port := 6666
	listenerChan := make(chan bool, nodeCount)
	servers := make([]net.Listener, nodeCount)
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	nodes := make([]*Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		laddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		if l, err := net.Listen("tcp", laddr); err != nil {
			t.Fatal(err)
		} else {
			port++
			servers[i] = l

			go func() {
				for {
					conn, err := l.Accept()
					if err != nil {
						if _, ok := err.(*net.OpError); !ok {
							t.Error(err)
						}
						return
					}
					go handleClientMessageWithRiakError(t, conn, listenerChan)
				}
			}()

			nodeOptions := &NodeOptions{
				RemoteAddress:  laddr,
				MinConnections: 0,
				MaxConnections: 1,
			}
			if node, err := NewNode(nodeOptions); err == nil {
				nodes[i] = node
			} else {
				t.Fatal(err)
			}
		}
	}

	clusterOptions := &ClusterOptions{
		Nodes: nodes,
	}
	cluster, err := NewCluster(clusterOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	fetch, err := NewFetchValueCommandBuilder().
		WithBucket("b").
		WithKey("k").
		Build()
	if err != nil {
		t.Fatal(err)
	}
	cluster.Execute(fetch)

	j := 0
	for j = 0; j < nodeCount; {
		select {
		case <-listenerChan:
			j++
		case <-time.After(5 * time.Second):
			t.Fatal("test timed out")
		}
	}
	if expected, actual := nodeCount, j; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
