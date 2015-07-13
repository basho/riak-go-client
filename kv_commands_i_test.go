// +build integration

package riak

import (
	"fmt"
	"strings"
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

// FetchValue

func TestFetchANotFoundFromRiakUsingDefaultBucketType(t *testing.T) {
	var err error
	var cmd Command
	builder := NewFetchValueCommandBuilder()
	if cmd, err = builder.WithBucket(testBucketName).WithKey("notfound_key").Build(); err != nil {
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

func TestFetchAValueFromRiakUsingDefaultBucketType(t *testing.T) {
	obj := &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("this is a value in Riak"),
	}
	store, err := NewStoreValueCommandBuilder().
		WithBucket(testBucketName).
		WithKey("my_key1").
		WithContent(obj).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(store); err != nil {
		t.Fatalf("error storing test object: %s", err.Error())
	}

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
		if expected, actual := false, rsp.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := false, rsp.IsUnchanged; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if rsp.VClock == nil {
			t.Errorf("expected non-nil VClock")
		}
		if rsp.Values == nil {
			t.Errorf("expected non-nil Values")
		}
		if expected, actual := 1, len(rsp.Values); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		object := rsp.Values[0]
		if expected, actual := "this is a value in Riak", string(object.Value); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "text/plain", object.ContentType; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "utf-8", object.Charset; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "utf-8", object.ContentEncoding; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// StoreValue
func TestStoreValueWithRiakGeneratedKey(t *testing.T) {
	obj := &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("value"),
	}
	cmd, err := NewStoreValueCommandBuilder().
		WithBucket(testBucketName).
		WithContent(obj).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if svc, ok := cmd.(*StoreValueCommand); ok {
		if svc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		rsp := svc.Response
		if rsp.GeneratedKey == "" {
			t.Error("expected non empty GeneratedKey")
		} else {
			t.Logf("GeneratedKey: %s", rsp.GeneratedKey)
		}
	} else {
		t.FailNow()
	}
}

// ListBuckets

func TestListBucketsInDefaultBucketType(t *testing.T) {
	bucketPrefix := "listBucketsInDefaultType_"
	obj := &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("value"),
	}
	for i := 0; i < 5; i++ {
		bucket := bucketPrefix + string(i)
		store, err := NewStoreValueCommandBuilder().
			WithBucket(bucket).
			WithContent(obj).
			Build()
		if err != nil {
			panic(err.Error())
		}
		if err := cluster.Execute(store); err != nil {
			t.Fatalf("error storing test objects: %s", err.Error())
		}
	}
	var err error
	var cmd Command
	builder := NewListBucketsCommandBuilder()
	if cmd, err = builder.WithBucketType(defaultBucketType).WithStreaming(false).Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if lbc, ok := cmd.(*ListBucketsCommand); ok {
		if lbc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		count := 0
		rsp := lbc.Response
		for _, b := range rsp.Buckets {
			if strings.HasPrefix(b, bucketPrefix) {
				count++
			}
		}
		if expected, actual := 5, count; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
