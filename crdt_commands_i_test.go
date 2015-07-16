// +build integration

package riak

import (
	"fmt"
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

// UpdateSet

func TestUpdateAndFetchSet(t *testing.T) {
	var err error
	var cmd Command

	adds := [][]byte{
		[]byte("a1"),
		[]byte("a2"),
		[]byte("a3"),
		[]byte("a4"),
	}

	b1 := NewUpdateSetCommandBuilder()
	cmd, err = b1.WithBucketType(testSetBucketType).
		WithBucket(testBucketName).
		WithReturnBody(true).
		WithAdditions(adds...).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateSetCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
			for i := 1; i <= 4; i++ {
				sitem := fmt.Sprintf("a%d", i)
				if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			}
		}
	} else {
		t.FailNow()
	}

	b2 := NewFetchSetCommandBuilder()
	cmd, err = b2.WithBucketType(testSetBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchSetCommand); ok {
		if fc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := fc.Response
		for i := 1; i <= 4; i++ {
			sitem := fmt.Sprintf("a%d", i)
			if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}
}
