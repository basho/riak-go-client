// +build integration

package riak

import (
	"testing"
)

func init() {
	integrationTestsBuildCluster()
}

// UpdateCounter

func TestUpdateAndFetchCounter(t *testing.T) {
	var err error
	var cmd Command

	b1 := NewUpdateCounterCommandBuilder()
	cmd, err = b1.WithBucketType(testCounterBucketType).
		WithBucket(testBucketName).
		WithReturnBody(true).
		WithIncrement(10).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateCounterCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
			if expected, actual := int64(10), rsp.CounterValue; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}

	b2 := NewFetchCounterCommandBuilder()
	cmd, err = b2.WithBucketType(testCounterBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchCounterCommand); ok {
		if expected, actual := int64(10), fc.Response.CounterValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
