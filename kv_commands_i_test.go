// +build integration

package riak

import (
	"fmt"
	"testing"
)

var cluster *Cluster

func init() {
	var err error
	if cluster, err = integrationTestsBuildCluster(); err != nil {
		panic(fmt.Sprintf("error building integration test cluster object: %s", err.Error()))
	} else {
		if err = cluster.Start(); err != nil {
			panic(fmt.Sprintf("error starting integration test cluster object: %s", err.Error()))
		}
	}
}

func TestFetchANotFoundFromRiakUsingDefaultBucketType(t *testing.T) {
	var err error
	var cmd Command
	builder := NewFetchValueCommandBuilder()
	if cmd, err = builder.WithBucket(testBucketName).WithKey("my_key1").Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fvc, ok := cmd.(*FetchValueCommand); ok {
		if fvc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		rsp := fvc.Response
		if expected, actual := true, rsp.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := false, rsp.IsUnchanged; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if rsp.VClock != nil {
			t.Errorf("expected nil VClock")
		}
		if rsp.Values != nil {
			t.Errorf("expected nil Values")
		}
		if expected, actual := 0, len(rsp.Values); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
