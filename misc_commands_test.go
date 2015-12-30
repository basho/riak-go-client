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
		if expected, actual := "bucket_type", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
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

	if expected, actual := true, cmd.Success(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	if fetchBucketPropsCommand, ok := cmd.(*FetchBucketPropsCommand); ok {
		if fetchBucketPropsCommand.Response == nil {
			t.Fatal("unexpected nil object")
		}
		if expected, actual := true, fetchBucketPropsCommand.success; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
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
		if expected, actual := "bucket_type", string(req.GetType()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket_name", string(req.GetBucket()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		props := req.Props
		if expected, actual := uint32val, props.GetNVal(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, props.GetAllowMult(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, props.GetLastWriteWins(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetOldVclock(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetYoungVclock(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetBigVclock(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetSmallVclock(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetR(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetPr(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetW(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetPw(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetDw(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, props.GetRw(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, props.GetBasicQuorum(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, props.GetNotfoundOk(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, props.GetSearch(); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "backend", string(props.GetBackend()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "index", string(props.GetSearchIndex()); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", string(props.ChashKeyfun.Module); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", string(props.ChashKeyfun.Function); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "hook_name", string(props.Precommit[0].Name); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", string(props.Precommit[0].Modfun.Module); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", string(props.Precommit[0].Modfun.Function); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "hook_name", string(props.Postcommit[0].Name); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", string(props.Postcommit[0].Modfun.Module); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", string(props.Postcommit[0].Modfun.Function); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
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

	if expected, actual := true, cmd.Success(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	if storeBucketPropsCommand, ok := cmd.(*StoreBucketPropsCommand); ok {
		if expected, actual := true, storeBucketPropsCommand.success; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
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
