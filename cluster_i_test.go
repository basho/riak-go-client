// +build integration

package riak

import (
	"testing"
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
		Nodes: nodes,
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
		logDebug("i: %d", i)
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
			logDebug("j: %d, pingCommand: %v", j, pingCommand)
			j++
		}
	}
	if expected, actual := count, j; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
