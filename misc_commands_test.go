package riak

import (
	"reflect"
	"testing"

	rpbRiak "github.com/basho/riak-go-client/rpb/riak"
)

var trueVal bool = true
var uint32val uint32 = uint32(9)
var replMode = rpbRiak.RpbBucketProps_REALTIME

var rpbBucketProps = &rpbRiak.RpbBucketProps{
	NVal:          &uint32val,
	AllowMult:     &trueVal,
	LastWriteWins: &trueVal,
	HasPrecommit:  &trueVal,
	HasPostcommit: &trueVal,
	OldVclock:     &uint32val,
	YoungVclock:   &uint32val,
	BigVclock:     &uint32val,
	SmallVclock:   &uint32val,
	R:             &uint32val,
	Pr:            &uint32val,
	W:             &uint32val,
	Pw:            &uint32val,
	Dw:            &uint32val,
	Rw:            &uint32val,
	BasicQuorum:   &trueVal,
	NotfoundOk:    &trueVal,
	Search:        &trueVal,
	Consistent:    &trueVal,
	Repl:          &replMode,
	Backend:       []byte("backend"),
	SearchIndex:   []byte("index"),
	Datatype:      []byte("datatype"),
}

var rpbModFun = &rpbRiak.RpbModFun{
	Module:   []byte("module_name"),
	Function: []byte("function_name"),
}

var rpbCommitHook = &rpbRiak.RpbCommitHook{
	Name:   []byte("hook_name"),
	Modfun: rpbModFun,
}

var rpbGetBucketResp = &rpbRiak.RpbGetBucketResp{}

func init() {
	rpbBucketProps.Precommit = []*rpbRiak.RpbCommitHook{rpbCommitHook}
	rpbBucketProps.Postcommit = []*rpbRiak.RpbCommitHook{rpbCommitHook}
	rpbBucketProps.ChashKeyfun = rpbModFun
	rpbBucketProps.Linkfun = rpbModFun
	rpbGetBucketResp.Props = rpbBucketProps
}

func validateFetchBucketPropsResponse(t *testing.T, r *FetchBucketPropsResponse) {
	if r == nil {
		t.Fatal("want non-nil response")
	}
	if got, want := r.NVal, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.AllowMult, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.LastWriteWins, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.HasPrecommit, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.HasPostcommit, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.OldVClock, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.YoungVClock, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.BigVClock, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.SmallVClock, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.R, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Pr, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.W, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Pw, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Dw, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Rw, uint32val; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.BasicQuorum, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.NotFoundOk, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Search, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Consistent, true; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := int32(r.Repl), int32(replMode); got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.Backend, "backend"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.SearchIndex, "index"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.DataType, "datatype"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PreCommit[0].Name, "hook_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PreCommit[0].ModFun.Module, "module_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PreCommit[0].ModFun.Function, "function_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PostCommit[0].Name, "hook_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PostCommit[0].ModFun.Module, "module_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.PostCommit[0].ModFun.Function, "function_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.ChashKeyFun.Module, "module_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.ChashKeyFun.Function, "function_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.LinkFun.Module, "module_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
	if got, want := r.LinkFun.Function, "function_name"; got != want {
		t.Errorf("want %v, got %v", got, want)
	}
}


// FetchBucketTypeProps

func TestBuildRpbGetBucketTypeReqCorrectlyViaBuilder(t *testing.T) {
	bt := "bucket_type"
	builder := NewFetchBucketTypePropsCommandBuilder().WithBucketType(bt)
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.Fatal("protobuf is nil")
	}
	if req, ok := protobuf.(*rpbRiak.RpbGetBucketTypeReq); ok {
		if got, want := string(req.GetType()), bt; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiak.RpbGetBucketTypeReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbGetBucketRespCorrectlyForBucketType(t *testing.T) {
	builder := NewFetchBucketTypePropsCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	err = cmd.onSuccess(rpbGetBucketResp)
	if err != nil {
		t.Fatal(err.Error())
	}

	if got, want := cmd.Success(), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if fetchBucketTypePropsCommand, ok := cmd.(*FetchBucketTypePropsCommand); ok {
		if fetchBucketTypePropsCommand.Response == nil {
			t.Fatal("unexpected nil object")
		}
		if got, want := fetchBucketTypePropsCommand.success, true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		validateFetchBucketPropsResponse(t, fetchBucketTypePropsCommand.Response)
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchBucketTypePropsCommand", ok, reflect.TypeOf(cmd))
	}
}

// FetchBucketProps

func TestBuildRpbGetBucketReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewFetchBucketPropsCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name")
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.Fatal("protobuf is nil")
	}
	if req, ok := protobuf.(*rpbRiak.RpbGetBucketReq); ok {
		if got, want := string(req.GetType()), "bucket_type"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(req.GetBucket()), "bucket_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiak.RpbGetBucketReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbGetBucketRespCorrectly(t *testing.T) {
	builder := NewFetchBucketPropsCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	err = cmd.onSuccess(rpbGetBucketResp)
	if err != nil {
		t.Fatal(err.Error())
	}

	if got, want := cmd.Success(), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if fetchBucketPropsCommand, ok := cmd.(*FetchBucketPropsCommand); ok {
		if fetchBucketPropsCommand.Response == nil {
			t.Fatal("unexpected nil object")
		}
		if got, want := fetchBucketPropsCommand.success, true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		validateFetchBucketPropsResponse(t, fetchBucketPropsCommand.Response)
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchBucketPropsCommand", ok, reflect.TypeOf(cmd))
	}
}

// StoreBucketProps

func TestBuildRpbStoreBucketReqCorrectlyViaBuilder(t *testing.T) {
	modFun := &ModFun{
		Module:   "module_name",
		Function: "function_name",
	}
	hook := &CommitHook{
		Name:   "hook_name",
		ModFun: modFun,
	}

	builder := NewStoreBucketPropsCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		WithNVal(uint32val).
		WithAllowMult(trueVal).
		WithLastWriteWins(trueVal).
		WithOldVClock(uint32val).
		WithYoungVClock(uint32val).
		WithBigVClock(uint32val).
		WithSmallVClock(uint32val).
		WithR(uint32val).
		WithPr(uint32val).
		WithW(uint32val).
		WithPw(uint32val).
		WithDw(uint32val).
		WithRw(uint32val).
		WithBasicQuorum(trueVal).
		WithNotFoundOk(trueVal).
		WithSearch(trueVal).
		WithBackend("backend").
		WithSearchIndex("index").
		AddPreCommit(hook).
		AddPostCommit(hook).
		WithChashKeyFun(modFun)

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.Fatal("protobuf is nil")
	}
	if req, ok := protobuf.(*rpbRiak.RpbSetBucketReq); ok {
		if got, want := string(req.GetType()), "bucket_type"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(req.GetBucket()), "bucket_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		props := req.Props
		if got, want := props.GetNVal(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetAllowMult(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetLastWriteWins(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetOldVclock(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetYoungVclock(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetBigVclock(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetSmallVclock(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetR(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetPr(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetW(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetPw(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetDw(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetRw(), uint32val; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetBasicQuorum(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetNotfoundOk(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := props.GetSearch(), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.GetBackend()), "backend"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.GetSearchIndex()), "index"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.ChashKeyfun.Module), "module_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.ChashKeyfun.Function), "function_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Precommit[0].Name), "hook_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Precommit[0].Modfun.Module), "module_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Precommit[0].Modfun.Function), "function_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Postcommit[0].Name), "hook_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Postcommit[0].Modfun.Module), "module_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(props.Postcommit[0].Modfun.Function), "function_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiak.RpbGetBucketReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbStoreBucketRespCorrectly(t *testing.T) {
	builder := NewStoreBucketPropsCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	err = cmd.onSuccess(nil)
	if err != nil {
		t.Fatal(err.Error())
	}

	if got, want := cmd.Success(), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if storeBucketPropsCommand, ok := cmd.(*StoreBucketPropsCommand); ok {
		if got, want := storeBucketPropsCommand.success, true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *StoreBucketPropsCommand", ok, reflect.TypeOf(cmd))
	}
}

// ResetBucket

func TestBuildRpbResetBucketReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewResetBucketCommandBuilder().
		WithBucketType("bucket_type").
		WithBucket("bucket_name")
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.Fatal("protobuf is nil")
	}
	if req, ok := protobuf.(*rpbRiak.RpbResetBucketReq); ok {
		if got, want := string(req.GetType()), "bucket_type"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(req.GetBucket()), "bucket_name"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiak.RpbResetBucketReq", ok, reflect.TypeOf(protobuf))
	}
}
