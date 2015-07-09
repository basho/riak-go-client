package riak

import (
	"bytes"
	rpbRiak "github.com/basho-labs/riak-go-client/rpb/riak"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"testing"
	"time"
)

var vclock = bytes.NewBufferString("vclock123456789")
var vclockBytes = vclock.Bytes()

// FetchValue

func TestBuildRpbGetReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewFetchValueCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey("key").
		WithR(3).
		WithPr(1).
		WithBasicQuorum(true).
		WithNotFoundOk(true).
		WithIfNotModified(vclockBytes).
		WithHeadOnly(true).
		WithReturnDeletedVClock(true).
		WithTimeout(time.Second * 20).
		WithSloppyQuorum(true).
		WithNVal(4)
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.FailNow()
	}
	validateRpbGetReq(t, protobuf)
}

func validateRpbGetReq(t *testing.T, protobuf proto.Message) {
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

func TestBuildRpbGetReqCorrectlyWithDefaults(t *testing.T) {
	builder := NewFetchValueCommandBuilder().
		WithBucket("bucket_name").
		WithKey("key")
	cmd, err := builder.Build()

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.FailNow()
	}
	if rpbGetReq, ok := protobuf.(*rpbRiakKV.RpbGetReq); ok {
		if expected, actual := "default", string(rpbGetReq.Type); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(rpbGetReq.Bucket); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(rpbGetReq.Key); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if rpbGetReq.R != nil {
			t.Errorf("expected nil value")
		}
		if rpbGetReq.Pr != nil {
			t.Errorf("expected nil value")
		}
		if rpbGetReq.NotfoundOk != nil {
			t.Error("expected nil value")
		}
		if rpbGetReq.IfModified != nil {
			t.Errorf("expected nil value")
		}
		if rpbGetReq.Head != nil {
			t.Error("expected nil value")
		}
		if rpbGetReq.Deletedvclock != nil {
			t.Error("expected nil value")
		}
		if rpbGetReq.Timeout != nil {
			t.Errorf("expected nil value")
		}
		if rpbGetReq.SloppyQuorum != nil {
			t.Error("expected nil value")
		}
		if rpbGetReq.NVal != nil {
			t.Errorf("expected nil value")
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

	builder := NewFetchValueCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey("key").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	cmd.onSuccess(rpbGetResp)

	if fetchValueCommand, ok := cmd.(*FetchValueCommand); ok {
		if fetchValueCommand.Response == nil {
			t.Error("unexpected nil object")
			t.FailNow()
		}
		if expected, actual := 1, len(fetchValueCommand.Response.Values); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		riakObject := fetchValueCommand.Response.Values[0]
		if riakObject == nil {
			t.Error("unexpected nil object")
			t.FailNow()
		}
		if expected, actual := "bucket_type", riakObject.BucketType; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "bucket_name", riakObject.Bucket; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "key", riakObject.Key; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "application/json", riakObject.ContentType; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "utf-8", riakObject.Charset; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "utf-8", riakObject.ContentEncoding; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "test-vtag", riakObject.VTag; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := time.Unix(1234, 123456789), riakObject.LastModified; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, riakObject.HasIndexes(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, riakObject.HasIndexes(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "golang@basho.com", riakObject.Indexes["email_bin"][0]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, riakObject.HasUserMeta(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "golang@basho.com", riakObject.Indexes["email_bin"][0]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "frazzle@basho.com", riakObject.Indexes["email_bin"][1]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "metaKey1", riakObject.UserMeta[0].Key; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "metaValue1", riakObject.UserMeta[0].Value; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "b0", riakObject.Links[0].Bucket; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "k0", riakObject.Links[0].Key; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "t0", riakObject.Links[0].Tag; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "b1", riakObject.Links[1].Bucket; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "k1", riakObject.Links[1].Key; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "t1", riakObject.Links[1].Tag; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "vclock123456789", string(riakObject.VClock); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchValueCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfRpbGetReqViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewFetchValueCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is required
	builder = NewFetchValueCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrKeyRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
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
		Indexes:         make([]*rpbRiak.RpbPair, 3),
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
		Key:   []byte("email_bin"),
		Value: []byte("frazzle@basho.com"),
	}
	rpbContent.Indexes[2] = &rpbRiak.RpbPair{
		Key:   []byte("phone_bin"),
		Value: []byte("15551234567"),
	}

	return rpbContent
}

// StoreValue

func TestValidationOfRpbPutReqViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewStoreValueCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is NOT required
	builder = NewStoreValueCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err != nil {
		t.Fatal("expected nil err since PUT requests can generate keys")
	}
}
