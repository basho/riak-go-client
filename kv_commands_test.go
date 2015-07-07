package riak

import (
	"bytes"
	rpbRiak "github.com/basho-labs/riak-go-client/rpb/riak"
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

func TestParseRpbGetRespCorrectly(t *testing.T) {
	rpbContent := generateTestRpbContent("this is a value", "application/json")
	vclock := bytes.NewBufferString("vclock123456789")

	rpbGetResp := &rpbRiakKV.RpbGetResp{
		Content: []*rpbRiakKV.RpbContent{rpbContent},
		Vclock:  vclock.Bytes(),
	}

	/*
		var callback = function(err, response) {
			assert(!err, err);
			assert(response, 'expected a response!');
			assert.equal(response.values.length, 1);
			var riakObject = response.values[0];
			assert.equal(riakObject.getBucketType(), 'bucket_type');
			assert.equal(riakObject.getBucket(), 'bucket_name');
			assert.equal(riakObject.getKey(), 'key');
			assert.equal(riakObject.getContentType(), 'application/json');
			assert.equal(riakObject.hasIndexes(), true);
			assert.equal(riakObject.getIndex('email_bin')[0], 'roach@basho.com');
			assert.equal(riakObject.hasUserMeta(), true);
			assert.equal(riakObject.getUserMeta()[0].key, 'metaKey1');
			assert.equal(riakObject.getUserMeta()[0].value, 'metaValue1');
			assert.equal(riakObject.getLinks()[0].bucket, 'b');
			assert.equal(riakObject.getLinks()[0].key, 'k');
			assert.equal(riakObject.getLinks()[0].tag, 't');
			assert.equal(riakObject.getLinks()[1].bucket, 'b');
			assert.equal(riakObject.getLinks()[1].key, 'k2');
			assert.equal(riakObject.getLinks()[1].tag, 't2');
			assert.equal(riakObject.getVClock().toString('utf8'), '1234');
			done();
		};
	*/

	fetchValueCommandBuilder := NewFetchValueCommandBuilder()
	fetchValueCommand, err := fetchValueCommandBuilder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey("key").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	fetchValueCommand.onSuccess(rpbGetResp)
}

func generateTestRpbContent(value string, contentType string) (rpbContent *rpbRiakKV.RpbContent) {
	lastMod := uint32(1234)
	lastModUsecs := uint32(123456789)
	deleted := false

	rpbContent = &rpbRiakKV.RpbContent{
		Value:           []byte(value),
		ContentType:     []byte(contentType),
		Charset:         []byte("utf-8"),
		ContentEncoding: []byte("utf-8"),
		Vtag:            []byte("test-vtag"),
		Links:           make([]*rpbRiakKV.RpbLink, 2),
		LastMod:         &lastMod,
		LastModUsecs:    &lastModUsecs,
		Usermeta:        make([]*rpbRiak.RpbPair, 2),
		Indexes:         make([]*rpbRiak.RpbPair, 2),
		Deleted:         &deleted,
	}

	rpbContent.Links[0] = &rpbRiakKV.RpbLink{
		Bucket: []byte("b0"),
		Key:    []byte("k0"),
		Tag:    []byte("t0"),
	}
	rpbContent.Links[1] = &rpbRiakKV.RpbLink{
		Bucket: []byte("b1"),
		Key:    []byte("k1"),
		Tag:    []byte("t1"),
	}

	rpbContent.Usermeta[0] = &rpbRiak.RpbPair{
		Key:   []byte("metaKey1"),
		Value: []byte("metaValue1"),
	}
	rpbContent.Usermeta[1] = &rpbRiak.RpbPair{
		Key:   []byte("metaKey2"),
		Value: []byte("metaValue2"),
	}

	rpbContent.Indexes[0] = &rpbRiak.RpbPair{
		Key:   []byte("email_bin"),
		Value: []byte("golang@basho.com"),
	}
	rpbContent.Indexes[1] = &rpbRiak.RpbPair{
		Key:   []byte("phone_bin"),
		Value: []byte("15551234567"),
	}

	return rpbContent
}
