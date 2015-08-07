package riak

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	rpbRiakDT "github.com/basho/riak-go-client/rpb/riak_dt"
)

// UpdateCounter
// DtUpdateReq
// DtUpdateResp

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
// DtFetchResp

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
		Type:  rpbRiakDT.DtFetchResp_COUNTER.Enum(),
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

// UpdateSet
// DtUpdateReq
// DtUpdateResp

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

func TestUpdateSetParsesDtUpdateRespCorrectly(t *testing.T) {
	setValue := [][]byte{
		[]byte("v1"),
		[]byte("v2"),
		[]byte("v3"),
		[]byte("v4"),
	}
	generatedKey := "generated_key"
	dtUpdateResp := &rpbRiakDT.DtUpdateResp{
		SetValue: setValue,
		Key:      []byte(generatedKey),
		Context:  crdtContextBytes,
	}

	builder := NewUpdateSetCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key")
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
		if expected, actual := 0, bytes.Compare(crdtContextBytes, rsp.Context); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		for i := 1; i <= 4; i++ {
			sitem := fmt.Sprintf("v%d", i)
			if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
		if expected, actual := "generated_key", rsp.GeneratedKey; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *UpdateSetCommand", ok, reflect.TypeOf(cmd))
	}
}

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

// FetchSet
// DtFetchReq
// DtFetchResp

func TestBuildDtFetchReqCorrectlyViaFetchSetCommandBuilder(t *testing.T) {
	builder := NewFetchSetCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key").
		WithR(1).
		WithPr(2).
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
		if expected, actual := "sets", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetR(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetPr(); expected != actual {
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
	dtValue := &rpbRiakDT.DtValue{
		SetValue: [][]byte{
			[]byte("v1"),
			[]byte("v2"),
			[]byte("v3"),
			[]byte("v4"),
		},
	}
	dtFetchResp := &rpbRiakDT.DtFetchResp{
		Type:    rpbRiakDT.DtFetchResp_SET.Enum(),
		Value:   dtValue,
		Context: crdtContextBytes,
	}
	builder := NewFetchSetCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key")
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

	if fc, ok := cmd.(*FetchSetCommand); ok {
		rsp := fc.Response
		if expected, actual := 0, bytes.Compare(crdtContextBytes, rsp.Context); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		for i := 1; i <= 4; i++ {
			sitem := fmt.Sprintf("v%d", i)
			if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
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

// UpdateMap
// DtUpdateReq
// DtUpdateResp

func createMapValue() []*rpbRiakDT.MapEntry {
	mapEntries := createMapEntries()
	innerMapEntries := createMapEntries()

	mapEntry := &rpbRiakDT.MapEntry{
		Field: &rpbRiakDT.MapField{
			Type: rpbRiakDT.MapField_MAP.Enum(),
			Name: []byte("map_1"),
		},
		MapValue: innerMapEntries,
	}

	return append(mapEntries, mapEntry)
}

func createMapEntries() []*rpbRiakDT.MapEntry {
	counterValue := int64(50)
	setValue := [][]byte{
		[]byte("value_1"),
		[]byte("value_2"),
	}
	flagValue := true

	mapEntries := []*rpbRiakDT.MapEntry{
		{
			Field: &rpbRiakDT.MapField{
				Type: rpbRiakDT.MapField_COUNTER.Enum(),
				Name: []byte("counter_1"),
			},
			CounterValue: &counterValue,
		},
		{
			Field: &rpbRiakDT.MapField{
				Type: rpbRiakDT.MapField_SET.Enum(),
				Name: []byte("set_1"),
			},
			SetValue: setValue,
		},
		{
			Field: &rpbRiakDT.MapField{
				Type: rpbRiakDT.MapField_REGISTER.Enum(),
				Name: []byte("register_1"),
			},
			RegisterValue: []byte("1234"),
		},
		{
			Field: &rpbRiakDT.MapField{
				Type: rpbRiakDT.MapField_FLAG.Enum(),
				Name: []byte("flag_1"),
			},
			FlagValue: &flagValue,
		},
	}

	return mapEntries
}

func TestBuildDtUpdateReqCorrectlyViaUpdateMapCommandBuilder(t *testing.T) {
	mapOp := &MapOperation{}
	mapOp.IncrementCounter("counter_1", 50).
		RemoveCounter("counter_2").
		AddToSet("set_1", []byte("set_value_1")).
		RemoveFromSet("set_2", []byte("set_value_2")).
		RemoveSet("set_3").
		SetRegister("register_1", []byte("register_value_1")).
		RemoveRegister("register_2").
		SetFlag("flag_1", true).
		RemoveFlag("flag_2").
		RemoveMap("map_3")

	mapOp.Map("map_2").
		IncrementCounter("counter_1", 50).
		RemoveCounter("counter_2").
		AddToSet("set_1", []byte("set_value_1")).
		RemoveFromSet("set_2", []byte("set_value_2")).
		RemoveSet("set_3").
		SetRegister("register_1", []byte("register_value_1")).
		RemoveRegister("register_2").
		SetFlag("flag_1", true).
		RemoveFlag("flag_2").
		RemoveMap("map_3")

	builder := NewUpdateMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("bucket").
		WithKey("key").
		WithContext(crdtContextBytes).
		WithMapOperation(mapOp).
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
		if expected, actual := "maps", string(req.GetType()); expected != actual {
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
		validateTimeout(t, time.Second*20, req.GetTimeout())

		mapOp := req.Op.MapOp

		verifyRemoves := func(removes []*rpbRiakDT.MapField) {
			if expected, actual := 5, len(removes); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			counterRemoved := false
			setRemoved := false
			registerRemoved := false
			flagRemoved := false
			mapRemoved := false
			for _, remove := range removes {
				switch remove.GetType() {
				case rpbRiakDT.MapField_COUNTER:
					if expected, actual := "counter_2", string(remove.Name); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					counterRemoved = true
				case rpbRiakDT.MapField_SET:
					if expected, actual := "set_3", string(remove.Name); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					setRemoved = true
				case rpbRiakDT.MapField_MAP:
					if expected, actual := "map_3", string(remove.Name); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					mapRemoved = true
				case rpbRiakDT.MapField_REGISTER:
					if expected, actual := "register_2", string(remove.Name); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					registerRemoved = true
				case rpbRiakDT.MapField_FLAG:
					if expected, actual := "flag_2", string(remove.Name); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					flagRemoved = true
				}
			}
			if expected, actual := true, counterRemoved; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, setRemoved; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, registerRemoved; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, flagRemoved; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, mapRemoved; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}

		verifyUpdates := func(updates []*rpbRiakDT.MapUpdate, expectMapUpdate bool) *rpbRiakDT.MapUpdate {
			counterIncremented := false
			setAddedTo := false
			setRemovedFrom := false
			registerSet := false
			flagSet := false
			mapAdded := false
			var mapUpdate *rpbRiakDT.MapUpdate
			for _, update := range updates {
				field := update.GetField()
				switch field.GetType() {
				case rpbRiakDT.MapField_COUNTER:
					if expected, actual := "counter_1", string(field.GetName()); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					if expected, actual := int64(50), update.CounterOp.GetIncrement(); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					counterIncremented = true
				case rpbRiakDT.MapField_SET:
					if len(update.SetOp.Adds) > 0 {
						if expected, actual := "set_1", string(field.GetName()); expected != actual {
							t.Errorf("expected %v, got %v", expected, actual)
						}
						if expected, actual := "set_value_1", string(update.SetOp.Adds[0]); expected != actual {
							t.Errorf("expected %v, got %v", expected, actual)
						}
						setAddedTo = true

					} else {
						if expected, actual := "set_2", string(field.GetName()); expected != actual {
							t.Errorf("expected %v, got %v", expected, actual)
						}
						if expected, actual := "set_value_2", string(update.SetOp.Removes[0]); expected != actual {
							t.Errorf("expected %v, got %v", expected, actual)
						}
						setRemovedFrom = true
					}
				case rpbRiakDT.MapField_MAP:
					if expectMapUpdate {
						if expected, actual := "map_2", string(field.GetName()); expected != actual {
							t.Errorf("expected %v, got %v", expected, actual)
						}
						mapAdded = true
						mapUpdate = update
					}
				case rpbRiakDT.MapField_REGISTER:
					if expected, actual := "register_1", string(field.GetName()); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					if expected, actual := "register_value_1", string(update.RegisterOp); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					registerSet = true
				case rpbRiakDT.MapField_FLAG:
					if expected, actual := "flag_1", string(field.GetName()); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					if expected, actual := rpbRiakDT.MapUpdate_ENABLE, update.GetFlagOp(); expected != actual {
						t.Errorf("expected %v, got %v", expected, actual)
					}
					flagSet = true
				}
			}

			if expected, actual := true, counterIncremented; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, setAddedTo; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, setRemovedFrom; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, registerSet; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, flagSet; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expectMapUpdate {
				if expected, actual := true, mapAdded; expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			} else {
				if expected, actual := false, mapAdded; expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			}

			return mapUpdate
		}

		verifyRemoves(mapOp.GetRemoves())
		innerMapUpdate := verifyUpdates(mapOp.GetUpdates(), true)
		verifyRemoves(innerMapUpdate.MapOp.GetRemoves())
		verifyUpdates(innerMapUpdate.MapOp.GetUpdates(), false)

	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtUpdateReq", ok, reflect.TypeOf(protobuf))
	}
}

func verifyMap(t *testing.T, m *Map) {
	if expected, actual := int64(50), m.Counters["counter_1"]; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "value_1", string(m.Sets["set_1"][0]); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "value_2", string(m.Sets["set_1"][1]); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "1234", string(m.Registers["register_1"]); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := true, m.Flags["flag_1"]; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestUpdateMapParsesDtUpdateRespCorrectly(t *testing.T) {
	generatedKey := "generated_key"
	dtUpdateResp := &rpbRiakDT.DtUpdateResp{
		Context:  crdtContextBytes,
		Key:      []byte(generatedKey),
		MapValue: createMapValue(),
	}

	builder := NewUpdateMapCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key").
		WithMapOperation(&MapOperation{})
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

	if uc, ok := cmd.(*UpdateMapCommand); ok {
		rsp := uc.Response
		if expected, actual := "generated_key", rsp.GeneratedKey; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, bytes.Compare(crdtContextBytes, rsp.Context); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		verifyMap(t, rsp.Map)
		verifyMap(t, rsp.Map.Maps["map_1"])
	} else {
		t.Errorf("ok: %v - could not convert %v to *UpdateMapCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfUpdateMapViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewUpdateMapCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is NOT required
	builder = NewUpdateMapCommandBuilder()
	builder.WithBucket("bucket_name")
	builder.WithMapOperation(&MapOperation{})
	_, err = builder.Build()
	if err != nil {
		t.Fatal("expected nil err")
	}

	// validate that context is required when removes are present
	op := &MapOperation{}
	op.RemoveSet("set_1")
	builder = NewUpdateMapCommandBuilder()
	builder.WithBucket("bucket_name")
	builder.WithMapOperation(op)
	_, err = builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
}

// FetchMap
// DtFetchReq
// DtFetchResp

func TestBuildDtFetchReqCorrectlyViaFetchMapCommandBuilder(t *testing.T) {
	builder := NewFetchMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("bucket").
		WithKey("key").
		WithR(1).
		WithPr(2).
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
		if expected, actual := "maps", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetR(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetPr(); expected != actual {
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

func TestFetchMapParsesDtFetchRespCorrectly(t *testing.T) {
	dtFetchResp := &rpbRiakDT.DtFetchResp{
		Type:    rpbRiakDT.DtFetchResp_MAP.Enum(),
		Context: crdtContextBytes,
		Value: &rpbRiakDT.DtValue{
			MapValue: createMapValue(),
		},
	}

	builder := NewFetchMapCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key")
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

	if fc, ok := cmd.(*FetchMapCommand); ok {
		rsp := fc.Response
		if expected, actual := 0, bytes.Compare(crdtContextBytes, rsp.Context); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		verifyMap(t, rsp.Map)
		verifyMap(t, rsp.Map.Maps["map_1"])
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchMapCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestFetchMapParsesDtFetchRespWithoutValueCorrectly(t *testing.T) {
	builder := NewFetchMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("bucket").
		WithKey("map_1")
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

	if uc, ok := cmd.(*FetchMapCommand); ok {
		if expected, actual := true, uc.Response.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchMapCommand", ok, reflect.TypeOf(cmd))
	}
}

func TestValidationOfFetchMapViaBuilder(t *testing.T) {
	// validate that Bucket is required
	builder := NewFetchMapCommandBuilder()
	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrBucketRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	// validate that Key is required
	builder = NewFetchMapCommandBuilder()
	builder.WithBucket("bucket_name")
	_, err = builder.Build()
	if err == nil {
		t.Fatal("expected non-nil err")
	}
	if expected, actual := ErrKeyRequired.Error(), err.Error(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}
}
