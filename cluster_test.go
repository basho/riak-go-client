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
}
