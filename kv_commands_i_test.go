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

func TestFetchAValueFromRiakUsingDefaultBucketType(t *testing.T) {
	builder := NewFetchValueCommandBuilder()
	if fetch, err := builder.WithBucket(testBucketName).WithKey("my_key1").Build(); err != nil {
		t.Error(err.Error())
	} else {
		if err := cluster.Execute(fetch); err != nil {
			t.Error(err.Error())
		}
	}
	/*
	   var fetch = new FetchValue.Builder()
	           .withBucket(Test.bucketName)
	           .withKey('my_key1')
	           .withCallback(callback)
	           .build();

	   cluster.execute(fetch);
	   var callback = function(err, resp) {
	       assert(!err, err);
	       assert.equal(resp.values.length, 1);
	       assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
	       assert.equal(resp.isNotFound, false);
	       done();
	   };
	*/
}
