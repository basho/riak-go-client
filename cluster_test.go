package riak

import (
	"fmt"
	"testing"
)

func TestCreateClusterWithDefaultOptions(t *testing.T) {
	cluster, err := NewCluster(nil)
	if err != nil {
		t.Error(err.Error())
	}
	if expected, actual := 1, len(cluster.nodes); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	defaultNodeAddr := cluster.nodes[0].addr.String()
	if expected, actual := defaultRemoteAddress, defaultNodeAddr; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := clusterCreated, cluster.getState(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if cluster.nodeManager == nil {
		t.Error("expected cluster to have a node manager")
	}
	if expected, actual := defaultExecutionAttempts, cluster.executionAttempts; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestCreateClusterWithFourNodes(t *testing.T) {
	nodes := make([]*Node, 0, 4)
	for port := 10017; port <= 10047; port += 10 {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		opts := &NodeOptions{
			RemoteAddress: addr,
		}
		if node, err := NewNode(opts); err != nil {
			t.Error(err.Error())
		} else {
			nodes = append(nodes, node)
		}
	}

	opts := &ClusterOptions{
		Nodes: nodes,
	}
	cluster, err := NewCluster(opts)
	if err != nil {
		t.Error(err.Error())
	}
	if expected, actual := 4, len(cluster.nodes); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	for i, node := range cluster.nodes {
		port := 10007 + ((i + 1) * 10)
		expectedAddr := fmt.Sprintf("127.0.0.1:%d", port)
		if expected, actual := expectedAddr, node.addr.String(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}
	if expected, actual := clusterCreated, cluster.getState(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if cluster.nodeManager == nil {
		t.Error("expected cluster to have a node manager")
	}
}

func ExampleNewCluster() {
	cluster, err := NewCluster(nil)
	if err != nil {
		panic(fmt.Sprintf("Error building cluster object: %s", err.Error()))
	}
	fmt.Println(cluster.nodes[0].addr.String())
	// Output: 127.0.0.1:8087
}
