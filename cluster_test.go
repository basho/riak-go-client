package riak

import (
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
	if expected, actual := CLUSTER_CREATED, cluster.state; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if cluster.nodeManager == nil {
		t.Error("expected cluster to have a node manager")
	}
}
