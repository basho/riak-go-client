// +build integration

package riak

import (
	"testing"
)

// UpdateHll

func TestUpdateAndFetchHll(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	var err error
	var cmd Command

	adds := [][]byte{
		[]byte("a1"),
		[]byte("a2"),
		[]byte("a3"),
		[]byte("a4"),
	}

	b1 := NewUpdateHllCommandBuilder()
	cmd, err = b1.WithBucketType(testHllBucketType).
		WithBucket(testBucketName).
		WithReturnBody(true).
		WithAdditions(adds...).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Skip(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateHllCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
			if expected, actual := uint64(4), rsp.Cardinality; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}

	b2 := NewFetchHllCommandBuilder()
	cmd, err = b2.WithBucketType(testHllBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchHllCommand); ok {
		if fc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := fc.Response
		if expected, actual := uint64(4), rsp.Cardinality; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

func TestFetchNotFoundHll(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	b2 := NewFetchHllCommandBuilder()
	cmd, err := b2.WithBucketType(testHllBucketType).
		WithBucket(testBucketName).
		WithKey("hll_not_found").
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Skip(err.Error())
	}
	if fc, ok := cmd.(*FetchHllCommand); ok {
		if fc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := fc.Response
		if expected, actual := uint64(0), rsp.Cardinality; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, rsp.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
