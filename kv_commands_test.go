package riak

import (
	"bytes"
	"testing"
	"time"
)

func TestBuildRpbGetReqCorrectly(t *testing.T) {
	vclock := bytes.NewBufferString("vclock123456789")

	fetchValueCommandOptions := &FetchValueCommandOptions{
		BucketType:          "bucket_type",
		Bucket:              "bucket_name",
		Key:                 "key",
		R:                   3,
		Pr:                  1,
		BasicQuorum:         true,
		NotFoundOk:          true,
		IfNotModified:       vclock.Bytes(),
		HeadOnly:            true,
		ReturnDeletedVClock: true,
		Timeout:             time.Second * 20,
		SloppyQuorum:        true,
		NVal:                4,
	}
	fetchValueCommand, err := NewFetchValueCommand(fetchValueCommandOptions)

	protobuf, err := fetchValueCommand.constructPbRequest()
	if err != nil {
		t.Error(err.Error())
	}
	if protobuf == nil {
		t.FailNow()
	}

	if expected, actual := "bucket_type", string(protobuf.GetType()); expected != actual {
		t.Errorf("expected %v, got %v")
	}
	if expected, actual := "bucket_name", string(protobuf.GetBucket()); expected != actual {
		t.Errorf("expected %v, got %v")
	}
	if expected, actual := "key", string(protobuf.GetKey()); expected != actual {
		t.Errorf("expected %v, got %v")
	}
	/*
		assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
		assert.equal(protobuf.getBucket().toString('utf8'), 'bucket_name');
		assert.equal(protobuf.getKey().toString('utf8'), 'key');
		assert.equal(protobuf.getR(), 3);
		assert.equal(protobuf.getPr(), 1);
		assert.equal(protobuf.getNotfoundOk(), true);
		assert.equal(protobuf.getBasicQuorum(), true);
		assert.equal(protobuf.getDeletedvclock(), true);
		assert.equal(protobuf.getHead(), true);
		assert(protobuf.getIfModified().toBuffer() !== null);
		assert.equal(protobuf.getTimeout(), 20000);
	*/
}
