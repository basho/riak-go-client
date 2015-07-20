// +build integration

package riak

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func init() {
	integrationTestsBuildCluster()
	addDataToIndexes()
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
		t.Errorf("Could not convert %v to *FetchValueCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestFetchAValueFromRiakUsingDefaultBucketType(t *testing.T) {
	obj := getBasicObject()
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
		t.Errorf("Could not convert %v to *FetchValueCommand", ok, reflect.TypeOf(cmd))
	}
}

// StoreValue
func TestStoreValueWithRiakGeneratedKey(t *testing.T) {
	obj := getBasicObject()
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
		t.Errorf("Could not convert %v to *StoreValueCommand", ok, reflect.TypeOf(cmd))
	}
}

// ListBuckets

func TestListBucketsInDefaultBucketType(t *testing.T) {
	totalCount := 50
	bucketPrefix := fmt.Sprintf("LBDT_%d", time.Now().Unix())
	obj := getBasicObject()
	for i := 0; i < totalCount; i++ {
		bucket := fmt.Sprintf("%s_%d", bucketPrefix, i)
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

	// non-streaming
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
		if expected, actual := totalCount, count; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}

	// streaming
	builder = NewListBucketsCommandBuilder()
	count := 0
	cb := func(buckets []string) error {
		for _, b := range buckets {
			if strings.HasPrefix(b, bucketPrefix) {
				count++
			}
		}
		return nil
	}
	if cmd, err = builder.WithStreaming(true).WithCallback(cb).Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if lbc, ok := cmd.(*ListBucketsCommand); ok {
		if lbc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		if expected, actual := totalCount, count; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *ListBucketsCommand", ok, reflect.TypeOf(cmd))
	}
}

// ListKeys

func TestListKeysInDefaultBucketType(t *testing.T) {
	totalCount := 50
	keyPrefix := fmt.Sprintf("LKDT_%d", time.Now().Unix())
	obj := getBasicObject()
	for i := 0; i < totalCount; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		store, err := NewStoreValueCommandBuilder().
			WithBucket(testBucketName).
			WithKey(key).
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
	// non-streaming
	builder := NewListKeysCommandBuilder()
	if cmd, err = builder.WithBucketType(defaultBucketType).WithBucket(testBucketName).WithStreaming(false).Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if lkc, ok := cmd.(*ListKeysCommand); ok {
		if lkc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		count := 0
		rsp := lkc.Response
		for _, k := range rsp.Keys {
			if strings.HasPrefix(k, keyPrefix) {
				count++
			}
		}
		if expected, actual := totalCount, count; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *ListKeysCommand", ok, reflect.TypeOf(cmd))
	}

	// streaming
	builder = NewListKeysCommandBuilder()
	count := 0
	cb := func(keys []string) error {
		for _, k := range keys {
			if strings.HasPrefix(k, keyPrefix) {
				count++
			}
		}
		return nil
	}
	if cmd, err = builder.WithBucket(testBucketName).WithStreaming(true).WithCallback(cb).Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if lbc, ok := cmd.(*ListKeysCommand); ok {
		if lbc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		if expected, actual := totalCount, count; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *ListKeysCommand", ok, reflect.TypeOf(cmd))
	}
}

// FetchPreflist

func TestFetchPreflistForAValue(t *testing.T) {
	key := fmt.Sprintf("FetchPreflist_%d", time.Now().Unix())
	obj := getBasicObject()
	store, err := NewStoreValueCommandBuilder().
		WithBucket(testBucketName).
		WithKey(key).
		WithContent(obj).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(store); err != nil {
		t.Fatalf("error storing test object: %s", err.Error())
	}

	var cmd Command
	builder := NewFetchPreflistCommandBuilder()
	if cmd, err = builder.WithBucket(testBucketName).WithKey(key).Build(); err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fpc, ok := cmd.(*FetchPreflistCommand); ok {
		if fpc.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		rsp := fpc.Response
		if rsp.Preflist == nil {
			t.Errorf("expected non-nil Preflist")
		}
		if expected, actual := 3, len(rsp.Preflist); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *FetchPreflistCommand", ok, reflect.TypeOf(cmd))
	}
}

// SecondaryIndexQueryCommand

func addDataToIndexes() {
	var store Command
	var err error
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key_%d", i)
		ro := &Object{
			ContentType: "text/plain",
			Value:       []byte("this is a value"),
		}
		idxVal := fmt.Sprintf("email%d", i)
		ro.AddToIndex("email_bin", idxVal)
		ro.AddToIntIndex("id_int", i)
		store, err = NewStoreValueCommandBuilder().
			WithBucket(testBucketName).
			WithKey(key).
			WithContent(ro).
			Build()
		if err != nil {
			panic(err.Error())
		}
		if err = cluster.Execute(store); err != nil {
			panic(err.Error())
		}

		ro = &Object{
			ContentType: "text/plain",
			Value:       []byte("this is a value"),
		}
		ro.AddToIndex("email_bin", idxVal)
		ro.AddToIntIndex("id_int", i)
		store, err := NewStoreValueCommandBuilder().
			WithBucketType(testBucketType).
			WithBucket(testBucketName).
			WithKey(key).
			WithContent(ro).
			Build()
		if err != nil {
			panic(err.Error())
		}
		if err = cluster.Execute(store); err != nil {
			panic(err.Error())
		}
	}
}

func TestIntQueryAgainstDefaultType(t *testing.T) {
	var cmd Command
	var err error
	cmd, err = NewSecondaryIndexQueryCommandBuilder().
		WithBucket(testBucketName).
		WithIndexName("id_int").
		WithIntRange(0, 10000).
		WithReturnKeyAndIndex(true).
		WithStreaming(false).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if q, ok := cmd.(*SecondaryIndexQueryCommand); ok {
		rsp := q.Response
		if expected, actual := 25, len(rsp.Results); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *SecondaryIndexQueryCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestIntQueryAgainstNonDefaultType(t *testing.T) {
	var cmd Command
	var err error
	cmd, err = NewSecondaryIndexQueryCommandBuilder().
		WithBucketType(testBucketType).
		WithBucket(testBucketName).
		WithIndexName("id_int").
		WithIntRange(0, 10000).
		WithReturnKeyAndIndex(true).
		WithStreaming(false).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if q, ok := cmd.(*SecondaryIndexQueryCommand); ok {
		rsp := q.Response
		if expected, actual := 25, len(rsp.Results); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *SecondaryIndexQueryCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestBinQueryAgainstDefaultType(t *testing.T) {
	var cmd Command
	var err error
	cmd, err = NewSecondaryIndexQueryCommandBuilder().
		WithBucket(testBucketName).
		WithIndexName("email_bin").
		WithRange("a", "z").
		WithReturnKeyAndIndex(true).
		WithStreaming(false).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if q, ok := cmd.(*SecondaryIndexQueryCommand); ok {
		rsp := q.Response
		if expected, actual := 25, len(rsp.Results); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *SecondaryIndexQueryCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestBinQueryAgainstNonDefaultType(t *testing.T) {
	var cmd Command
	var err error
	cmd, err = NewSecondaryIndexQueryCommandBuilder().
		WithBucketType(testBucketType).
		WithBucket(testBucketName).
		WithIndexName("email_bin").
		WithRange("a", "z").
		WithReturnKeyAndIndex(true).
		WithStreaming(false).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if q, ok := cmd.(*SecondaryIndexQueryCommand); ok {
		rsp := q.Response
		if expected, actual := 25, len(rsp.Results); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("Could not convert %v to *SecondaryIndexQueryCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestSetContinuationOnPaginatedQuery(t *testing.T) {
	var cmd Command
	var err error
	cmd, err = NewSecondaryIndexQueryCommandBuilder().
		WithBucketType(testBucketType).
		WithBucket(testBucketName).
		WithIndexName("email_bin").
		WithRange("a", "z").
		WithMaxResults(10).
		WithReturnKeyAndIndex(true).
		WithStreaming(false).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if q, ok := cmd.(*SecondaryIndexQueryCommand); ok {
		rsp := q.Response
		if expected, actual := 10, len(rsp.Results); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if rsp.Continuation == nil {
			t.Error("expected non-nil continuation.")
		}
	} else {
		t.Errorf("Could not convert %v to *SecondaryIndexQueryCommand", ok, reflect.TypeOf(cmd))
	}
}
