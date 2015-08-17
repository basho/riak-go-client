// +build integration

package riak

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
	// "runtime"
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
					go handleClientMessageWithRiakError(t, conn, 1, listenerChan)
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

	cmd, err := NewFetchValueCommandBuilder().
		WithBucket("b").
		WithKey("k").
		Build()
	if err != nil {
		t.Fatal(err)
	}
	cluster.Execute(cmd)

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
	fetch := cmd.(*FetchValueCommand)
	if expected, actual := byte(0), fetch.remainingTries; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestAsyncExecuteCommandOnCluster(t *testing.T) {
	nodeOpts := &NodeOptions{
		RemoteAddress: getRiakAddress(),
	}

	var node *Node
	var err error
	if node, err = NewNode(nodeOpts); err != nil {
		t.Fatal(err.Error())
	}
	if node == nil {
		t.FailNow()
	}

	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes:             nodes,
		ExecutionAttempts: 3,
	}

	cluster, err := NewCluster(opts)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	if err := cluster.Start(); err != nil {
		t.Fatal(err.Error())
	}

	command := &PingCommand{}
	args := &Async{
		Command: command,
		Done:    make(chan Command),
	}
	if err := cluster.ExecuteAsync(args); err != nil {
		t.Fatal(err.Error())
	}

	done := <-args.Done
	pingDone := done.(*PingCommand)

	if expected, actual := true, command == pingDone; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
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

	command = &PingCommand{}
	wg := &sync.WaitGroup{}
	args = &Async{
		Command: command,
		Wait:    wg,
	}
	if err := cluster.ExecuteAsync(args); err != nil {
		t.Fatal(err.Error())
	}

	wg.Wait()
	if expected, actual := true, command.Successful(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

/*
func TestEnqueueCommandsAndRetryFromQueue(t *testing.T) {
	runtime.GOMAXPROCS(8)
	var err error
	addr := "127.0.0.1:1340"
	pingCount := uint16(3)

	nodeOpts := &NodeOptions{
		RemoteAddress: addr,
		ConnectTimeout: 250 * time.Millisecond,
		RequestTimeout: 250 * time.Millisecond,
	}

	var node *Node
	if node, err = NewNode(nodeOpts); err != nil {
		t.Fatal(err.Error())
	}
	if node == nil {
		t.FailNow()
	}

	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes:         nodes,
		QueueMaxDepth: 0,
	}

	var cluster *Cluster
	cluster, err = NewCluster(opts)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	if err = cluster.Start(); err != nil {
		t.Fatal(err.Error())
	}

	command := &PingCommand{}
	args := &Async{
		Command: command,
		Done:    make(chan Command),
	}
	if err = cluster.ExecuteAsync(args); err != nil {
		t.Fatal(err.Error())
	}

	listenerChan := make(chan bool, pingCount)
	var ln net.Listener
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if _, ok := err.(*net.OpError); !ok {
					t.Error(err)
				}
				return
			}
			logDebugln("[TestEnqueueCommandsAndRetryFromQueue]", "ACCEPT / HANDLING")
			go handleClientMessageWithRiakError(t, conn, pingCount, listenerChan)
		}
	}()

	done := <-args.Done
	pingDone := done.(*PingCommand)
	listened := <-listenerChan

	if expected, actual := true, command == pingDone; expected != actual {
		t.Errorf("listened: %v, expected %v, got %v", listened, expected, actual)
	}
	if expected, actual := true, command.Successful(); expected != actual {
		t.Errorf("listened: %v, expected %v, got %v", listened, expected, actual)
	}
}
*/
