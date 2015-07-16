package riak

import (
	"bytes"
	"fmt"
	rpbRiakDT "github.com/basho-labs/riak-go-client/rpb/riak_dt"
	"reflect"
	"testing"
	"time"
)

// UpdateCounter
// DtUpdateReq

func TestBuildDtUpdateReqCorrectlyViaUpdateCounterCommandBuilder(t *testing.T) {
	builder := NewUpdateCounterCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1").
		WithIncrement(100).
		WithW(3).
		WithPw(1).
		WithDw(2).
		WithReturnBody(true).
		WithTimeout(time.Second * 20)
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
	if req, ok := protobuf.(*rpbRiakDT.DtUpdateReq); ok {
		if expected, actual := "counters", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "myBucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "counter_1", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(3), req.GetW(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetPw(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetDw(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetReturnBody(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		op := req.Op.CounterOp
		if expected, actual := int64(100), op.GetIncrement(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		validateTimeout(t, time.Second*20, req.GetTimeout())
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtUpdateReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestUpdateCounterParsesDtUpdateRespCorrectly(t *testing.T) {
	counterValue := int64(1234)
	generatedKey := "generated_key"
	dtUpdateResp := &rpbRiakDT.DtUpdateResp{
		CounterValue: &counterValue,
		Key:          []byte(generatedKey),
	}

	builder := NewUpdateCounterCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1").
		WithIncrement(100)
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

	cmd.onSuccess(dtUpdateResp)

	if uc, ok := cmd.(*UpdateCounterCommand); ok {
		rsp := uc.Response
		if expected, actual := int64(1234), rsp.CounterValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "generated_key", rsp.GeneratedKey; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *UpdateCounterCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfUpdateCounterViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewUpdateCounterCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is NOT required
	builder = NewUpdateCounterCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err != nil {
		t.Fatal("expected nil err")
	}
}

// FetchCounter
// DtFetchReq

func TestBuildDtFetchReqCorrectlyViaFetchCounterCommandBuilder(t *testing.T) {
	builder := NewFetchCounterCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1").
		WithR(3).
		WithPr(1).
		WithNotFoundOk(true).
		WithBasicQuorum(true).
		WithTimeout(time.Second * 20)
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
	if req, ok := protobuf.(*rpbRiakDT.DtFetchReq); ok {
		if expected, actual := "counters", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "myBucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "counter_1", string(req.GetKey()); expected != actual {
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
		if expected, actual := true, req.GetBasicQuorum(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		validateTimeout(t, time.Second*20, req.GetTimeout())
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtFetchReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestFetchCounterParsesDtFetchRespCorrectly(t *testing.T) {
	counterValue := int64(1234)
	dtValue := &rpbRiakDT.DtValue{
		CounterValue: &counterValue,
	}
	dtFetchResp := &rpbRiakDT.DtFetchResp{
		Value: dtValue,
	}

	builder := NewFetchCounterCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1")
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

	cmd.onSuccess(dtFetchResp)

	if uc, ok := cmd.(*FetchCounterCommand); ok {
		rsp := uc.Response
		if expected, actual := counterValue, rsp.CounterValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchCounterCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestFetchCounterParsesDtFetchRespWithoutValueCorrectly(t *testing.T) {
	builder := NewFetchCounterCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1")
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

	dtFetchResp := &rpbRiakDT.DtFetchResp{}
	cmd.onSuccess(dtFetchResp)

	if uc, ok := cmd.(*FetchCounterCommand); ok {
		if expected, actual := true, uc.Response.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchCounterCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfFetchCounterViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewFetchCounterCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is required
	builder = NewFetchCounterCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrKeyRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
}

func sliceIncludes(slice [][]byte, term []byte) (rv bool) {
	rv = false
	for _, t := range slice {
		if bytes.Compare(t, term) == 0 {
			rv = true
			break
		}
	}
	return
}

// UpdateSet
// DtUpdateReq

func TestBuildDtUpdateReqCorrectlyViaUpdateSetCommandBuilder(t *testing.T) {
	builder := NewUpdateSetCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key").
		WithContext(crdtContextBytes).
		WithAdditions([]byte("a1"), []byte("a2")).
		WithAdditions([]byte("a3"), []byte("a4")).
		WithRemovals([]byte("r1"), []byte("r2")).
		WithRemovals([]byte("r3"), []byte("r4")).
		WithW(1).
		WithDw(2).
		WithPw(3).
		WithReturnBody(true).
		WithTimeout(time.Second * 20)
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
	if req, ok := protobuf.(*rpbRiakDT.DtUpdateReq); ok {
		if expected, actual := "sets", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, bytes.Compare(crdtContextBytes, req.GetContext()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetW(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetDw(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(3), req.GetPw(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		validateTimeout(t, time.Second*20, req.GetTimeout())

		op := req.Op.SetOp

		for i := 1; i <= 4; i++ {
			aitem := fmt.Sprintf("a%d", i)
			ritem := fmt.Sprintf("r%d", i)
			if expected, actual := true, sliceIncludes(op.Adds, []byte(aitem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, sliceIncludes(op.Removes, []byte(ritem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtUpdateReq", ok, reflect.TypeOf(protobuf))
	}
}

/*
func TestUpdateSetParsesDtUpdateRespCorrectly(t *testing.T) {
	counterValue := int64(1234)
	generatedKey := "generated_key"
	dtUpdateResp := &rpbRiakDT.DtUpdateResp{
		SetValue: &counterValue,
		Key:          []byte(generatedKey),
	}

	builder := NewUpdateSetCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1").
		WithIncrement(100)
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

	cmd.onSuccess(dtUpdateResp)

	if uc, ok := cmd.(*UpdateSetCommand); ok {
		rsp := uc.Response
		if expected, actual := int64(1234), rsp.SetValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "generated_key", rsp.GeneratedKey; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *UpdateSetCommand", ok, reflect.TypeOf(cmd))
	}
}
*/

func TestValidationOfUpdateSetViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewUpdateSetCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is NOT required
	builder = NewUpdateSetCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err != nil {
		t.Fatal("expected nil err")
	}
}

/*
// FetchSet
// DtFetchReq

func TestBuildDtFetchReqCorrectlyViaFetchSetCommandBuilder(t *testing.T) {
	builder := NewFetchSetCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1").
		WithR(3).
		WithPr(1).
		WithNotFoundOk(true).
		WithBasicQuorum(true).
		WithTimeout(time.Second * 20)
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
	if req, ok := protobuf.(*rpbRiakDT.DtFetchReq); ok {
		if expected, actual := "counters", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "myBucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "counter_1", string(req.GetKey()); expected != actual {
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
		if expected, actual := true, req.GetBasicQuorum(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		validateTimeout(t, time.Second*20, req.GetTimeout())
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtFetchReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestFetchSetParsesDtFetchRespCorrectly(t *testing.T) {
	counterValue := int64(1234)
	dtValue := &rpbRiakDT.DtValue{
		SetValue: &counterValue,
	}
	dtFetchResp := &rpbRiakDT.DtFetchResp{
		Value: dtValue,
	}

	builder := NewFetchSetCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1")
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

	cmd.onSuccess(dtFetchResp)

	if uc, ok := cmd.(*FetchSetCommand); ok {
		rsp := uc.Response
		if expected, actual := counterValue, rsp.SetValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchSetCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestFetchSetParsesDtFetchRespWithoutValueCorrectly(t *testing.T) {
	builder := NewFetchSetCommandBuilder().
		WithBucketType("counters").
		WithBucket("myBucket").
		WithKey("counter_1")
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

	dtFetchResp := &rpbRiakDT.DtFetchResp{}
	cmd.onSuccess(dtFetchResp)

	if uc, ok := cmd.(*FetchSetCommand); ok {
		if expected, actual := true, uc.Response.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchSetCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfFetchSetViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewFetchSetCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is required
	builder = NewFetchSetCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrKeyRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
}
*/
