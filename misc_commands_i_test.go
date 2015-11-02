// +build integration

package riak

import (
	"testing"
)

// Update and Reset bucket properties

func TestSetAndResetBucketProperties(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	const bucket = "set-reset-bucket-props"
	orig_nval := uint32(3)
	new_nval := uint32(9)

	var err error
	var cmd Command

	cmd, err = NewStoreBucketPropsCommandBuilder().
		WithBucket(bucket).
		WithNVal(new_nval).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if sc, ok := cmd.(*StoreBucketPropsCommand); ok {
		if got, want := sc.Success(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.FailNow()
	}

	cmd, err = NewFetchBucketPropsCommandBuilder().
		WithBucket(bucket).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchBucketPropsCommand); ok {
		if got, want := fc.Success(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := fc.Response.NVal, new_nval; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.FailNow()
	}

	cmd, err = NewResetBucketCommandBuilder().
		WithBucket(bucket).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if rc, ok := cmd.(*ResetBucketCommand); ok {
		if got, want := rc.Success(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.FailNow()
	}

	cmd, err = NewFetchBucketPropsCommandBuilder().
		WithBucket(bucket).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchBucketPropsCommand); ok {
		if got, want := fc.Success(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := fc.Response.NVal, orig_nval; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.FailNow()
	}
}
