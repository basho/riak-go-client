package riak

import (
	"bytes"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	"reflect"
	"testing"
	"time"
)

func TestBuildRpbGetReqCorrectly(t *testing.T) {
	vclock := bytes.NewBufferString("vclock123456789")
	vclockBytes := vclock.Bytes()

	fetchValueCommandOptions := &FetchValueCommandOptions{
		BucketType:          "bucket_type",
		Bucket:              "bucket_name",
		Key:                 "key",
		R:                   3,
		Pr:                  1,
		BasicQuorum:         true,
		NotFoundOk:          true,
		IfNotModified:       vclockBytes, // TODO pb is IfModified?
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

	if rpbGetReq, ok := protobuf.(*rpbRiakKV.RpbGetReq); ok {
		if expected, actual := "bucket_type", string(rpbGetReq.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(rpbGetReq.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(rpbGetReq.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(3), rpbGetReq.GetR(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), rpbGetReq.GetPr(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, rpbGetReq.GetNotfoundOk(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, bytes.Compare(vclockBytes, rpbGetReq.GetIfModified()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, rpbGetReq.GetHead(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, rpbGetReq.GetDeletedvclock(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		expectedTimeoutDuration := 20 * time.Second
		actualTimeoutDuration := time.Duration(rpbGetReq.GetTimeout()) * time.Millisecond
		if expected, actual := expectedTimeoutDuration, actualTimeoutDuration; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, rpbGetReq.GetSloppyQuorum(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(4), rpbGetReq.GetNVal(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbGetReq", ok, reflect.TypeOf(protobuf))
	}
}
