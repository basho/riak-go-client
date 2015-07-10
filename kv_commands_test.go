package riak

import (
	"bytes"
	"fmt"
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
	if req, ok := protobuf.(*rpbRiakKV.RpbGetReq); ok {
		if expected, actual := "bucket_type", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(3), req.GetR(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetPr(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetNotfoundOk(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, bytes.Compare(vclockBytes, req.GetIfModified()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetHead(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetDeletedvclock(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		expectedTimeoutDuration := 20 * time.Second
		actualTimeoutDuration := time.Duration(req.GetTimeout()) * time.Millisecond
		if expected, actual := expectedTimeoutDuration, actualTimeoutDuration; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetSloppyQuorum(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(4), req.GetNVal(); expected != actual {
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
	if req, ok := protobuf.(*rpbRiakKV.RpbGetReq); ok {
		if expected, actual := "default", string(req.Type); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(req.Bucket); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.Key); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if req.R != nil {
			t.Errorf("expected nil value")
		}
		if req.Pr != nil {
			t.Errorf("expected nil value")
		}
		if req.NotfoundOk != nil {
			t.Error("expected nil value")
		}
		if req.IfModified != nil {
			t.Errorf("expected nil value")
		}
		if req.Head != nil {
			t.Error("expected nil value")
		}
		if req.Deletedvclock != nil {
			t.Error("expected nil value")
		}
		if req.Timeout != nil {
			t.Errorf("expected nil value")
		}
		if req.SloppyQuorum != nil {
			t.Error("expected nil value")
		}
		if req.NVal != nil {
			t.Errorf("expected nil value")
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbGetReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbGetRespCorrectly(t *testing.T) {
	rpbContent := generateTestRpbContent("this is a value", "application/json")

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

func TestBuildRpbPutReqCorrectlyViaBuilder(t *testing.T) {
	value := "this is a value"
	userMeta := []*Pair{
		{"metaKey1", "metaValue1"},
		{"metaKey2", "metaValue2"},
	}
	links := []*Link{
		{"b0", "k0", "t0"},
		{"b1", "k1", "t1"},
	}
	ro := &Object{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Charset:         "utf-8",
		UserMeta:        userMeta,
		Links:           links,
		Value:           []byte(value),
	}
	ro.AddToIndex("email_bin", "golang@basho.com")
	ro.AddToIndex("email_bin", "frazzle@basho.com")

	key := "key"
	builder := NewStoreValueCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey(key).
		WithW(3).
		WithPw(1).
		WithDw(2).
		WithNVal(3).
		WithVClock(vclockBytes).
		WithReturnHead(true).
		WithReturnBody(true).
		WithIfNotModified(true).
		WithIfNoneMatch(true).
		WithAsis(true).
		WithSloppyQuorum(true).
		WithTimeout(time.Second * 20).
		WithContent(ro)
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}

	if req, ok := protobuf.(*rpbRiakKV.RpbPutReq); ok {
		if expected, actual := "bucket_type", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := key, string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32(3), req.GetW(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetPw(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetDw(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32(3), req.GetNVal(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := 0, bytes.Compare(vclockBytes, req.GetVclock()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetReturnHead(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetReturnBody(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetIfNotModified(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetIfNoneMatch(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetAsis(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetSloppyQuorum(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		expectedTimeoutDuration := 20 * time.Second
		actualTimeoutDuration := time.Duration(req.GetTimeout()) * time.Millisecond
		if expected, actual := expectedTimeoutDuration, actualTimeoutDuration; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		content := req.GetContent()
		if content == nil {
			t.Fatal("expected non-nil content")
		} else {
			if expected, actual := value, string(content.GetValue()); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "application/json", string(content.GetContentType()); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "gzip", string(content.GetContentEncoding()); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "utf-8", string(content.GetCharset()); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			indexes := content.GetIndexes()
			if expected, actual := 2, len(indexes); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "email_bin", string(indexes[0].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "golang@basho.com", string(indexes[0].Value); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "email_bin", string(indexes[1].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "frazzle@basho.com", string(indexes[1].Value); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			usermeta := content.GetUsermeta()
			if expected, actual := 2, len(usermeta); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "metaKey1", string(usermeta[0].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "metaValue1", string(usermeta[0].Value); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "metaKey2", string(usermeta[1].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "metaValue2", string(usermeta[1].Value); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			links := content.GetLinks()
			if expected, actual := 2, len(links); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "b0", string(links[0].Bucket); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "k0", string(links[0].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "t0", string(links[0].Tag); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "b1", string(links[1].Bucket); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "k1", string(links[1].Key); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "t1", string(links[1].Tag); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbPutReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestBuildRpbPutReqUsingObjectValues(t *testing.T) {
	objectVclock := "object_vclock"
	ro := &Object{
		BucketType: "object_bucket_type",
		Bucket:     "object_bucket_name",
		Key:        "object_key",
		VClock:     []byte(objectVclock),
	}

	builder := NewStoreValueCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey("key").
		WithVClock([]byte("vclock")).
		WithContent(ro)
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}

	if req, ok := protobuf.(*rpbRiakKV.RpbPutReq); ok {
		if expected, actual := "object_bucket_type", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "object_bucket_name", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "object_key", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "object_vclock", string(req.GetVclock()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbPutReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbPutRespCorrectly(t *testing.T) {
	rpbContent := &rpbRiakKV.RpbContent{
		Value:           []byte("this is a value"),
		ContentType:     []byte("text/plain"),
		ContentEncoding: []byte("ascii"),
		Charset:         []byte("ascii"),
		Links: []*rpbRiakKV.RpbLink{
			{[]byte("b0"), []byte("k0"), []byte("t0"), nil},
			{[]byte("b1"), []byte("k1"), []byte("t1"), nil},
		},
		Usermeta: []*rpbRiak.RpbPair{
			{[]byte("metaKey0"), []byte("metaValue0"), nil},
			{[]byte("metaKey1"), []byte("metaValue1"), nil},
		},
		Indexes: []*rpbRiak.RpbPair{
			{[]byte("email_bin"), []byte("golang@basho.com"), nil},
			{[]byte("email_bin"), []byte("frazzle@basho.com"), nil},
			{[]byte("test_int"), []byte("1"), nil},
			{[]byte("test_int"), []byte("2"), nil},
		},
	}

	rpbPutResp := &rpbRiakKV.RpbPutResp{
		Content: []*rpbRiakKV.RpbContent{rpbContent},
		Vclock:  vclock.Bytes(),
		Key:     []byte("generated_riak_key"),
	}

	builder := NewStoreValueCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithKey("ignored_key").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	cmd.onSuccess(rpbPutResp)

	if storeValueCommand, ok := cmd.(*StoreValueCommand); ok {
		if storeValueCommand.Response == nil {
			t.Error("unexpected nil object")
			t.FailNow()
		}
		if expected, actual := 1, len(storeValueCommand.Response.Values); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		ro := storeValueCommand.Response.Values[0]
		if ro == nil {
			t.Error("unexpected nil object")
			t.FailNow()
		}
		if expected, actual := "bucket_type", ro.BucketType; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "bucket_name", ro.Bucket; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "generated_riak_key", ro.Key; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "text/plain", ro.ContentType; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "ascii", ro.Charset; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "ascii", ro.ContentEncoding; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, ro.HasLinks(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := 2, len(ro.Links); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		for i, link := range ro.Links {
			bucket := fmt.Sprintf("b%d", i)
			if expected, actual := bucket, string(link.Bucket); expected != actual {
				t.Errorf("expected %v, actual %v", expected, actual)
			}
			key := fmt.Sprintf("k%d", i)
			if expected, actual := key, string(link.Key); expected != actual {
				t.Errorf("expected %v, actual %v", expected, actual)
			}
			tag := fmt.Sprintf("t%d", i)
			if expected, actual := tag, string(link.Tag); expected != actual {
				t.Errorf("expected %v, actual %v", expected, actual)
			}
		}
		if expected, actual := true, ro.HasUserMeta(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := 2, len(ro.UserMeta); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		for i, meta := range ro.UserMeta {
			key := fmt.Sprintf("metaKey%d", i)
			if expected, actual := key, string(meta.Key); expected != actual {
				t.Errorf("expected %v, actual %v", expected, actual)
			}
			value := fmt.Sprintf("metaValue%d", i)
			if expected, actual := value, string(meta.Value); expected != actual {
				t.Errorf("expected %v, actual %v", expected, actual)
			}
		}
		if expected, actual := true, ro.HasIndexes(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := 2, len(ro.Indexes); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "golang@basho.com", ro.Indexes["email_bin"][0]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "frazzle@basho.com", ro.Indexes["email_bin"][1]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "1", ro.Indexes["test_int"][0]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "2", ro.Indexes["test_int"][1]; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchValueCommand", ok, reflect.TypeOf(cmd))
	}
}
