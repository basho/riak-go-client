package riak

import (
	rpbRiak "github.com/basho/riak-go-client/rpb/riak"
	"reflect"
	"testing"
)

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
		t.FailNow()
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
	trueVal := true
	uint32val := uint32(9)
	replMode := rpbRiak.RpbBucketProps_REALTIME

	rpbBucketProps := &rpbRiak.RpbBucketProps{
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

	rpbModFun := &rpbRiak.RpbModFun{
		Module:   []byte("module_name"),
		Function: []byte("function_name"),
	}

	rpbCommitHook := &rpbRiak.RpbCommitHook{
		Name:   []byte("hook_name"),
		Modfun: rpbModFun,
	}

	rpbBucketProps.Precommit = []*rpbRiak.RpbCommitHook{rpbCommitHook}
	rpbBucketProps.Postcommit = []*rpbRiak.RpbCommitHook{rpbCommitHook}

	rpbBucketProps.ChashKeyfun = rpbModFun
	rpbBucketProps.Linkfun = rpbModFun

	rpbGetBucketResp := &rpbRiak.RpbGetBucketResp{
		Props: rpbBucketProps,
	}

	builder := NewFetchBucketPropsCommandBuilder()
	cmd, err := builder.
		WithBucketType("bucket_type").
		WithBucket("bucket_name").
		Build()
	if err != nil {
		t.Error(err.Error())
	}

	cmd.onSuccess(rpbGetBucketResp)
	if expected, actual := true, cmd.Successful(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	if fetchBucketPropsCommand, ok := cmd.(*FetchBucketPropsCommand); ok {
		if fetchBucketPropsCommand.Response == nil {
			t.Error("unexpected nil object")
			t.FailNow()
		}
		if expected, actual := true, fetchBucketPropsCommand.Success; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if fetchBucketPropsCommand.Response == nil {
			t.Fatal("expected non-nil response")
		}
		response := fetchBucketPropsCommand.Response
		if expected, actual := uint32val, response.NVal; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.AllowMult; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.LastWriteWins; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.HasPrecommit; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.HasPostcommit; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.OldVClock; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.YoungVClock; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.BigVClock; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.SmallVClock; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.R; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.Pr; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.W; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.Pw; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.Dw; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := uint32val, response.Rw; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.BasicQuorum; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.NotFoundOk; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.Search; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := true, response.Consistent; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := int32(replMode), int32(response.Repl); expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "backend", response.Backend; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "index", response.SearchIndex; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "datatype", response.DataType; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "hook_name", response.PreCommit[0].Name; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", response.PreCommit[0].ModFun.Module; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", response.PreCommit[0].ModFun.Function; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "hook_name", response.PostCommit[0].Name; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", response.PostCommit[0].ModFun.Module; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", response.PostCommit[0].ModFun.Function; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", response.ChashKeyFun.Module; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", response.ChashKeyFun.Function; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "module_name", response.LinkFun.Module; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
		if expected, actual := "function_name", response.LinkFun.Function; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchBucketPropsCommand", ok, reflect.TypeOf(cmd))
	}
}

// StoreBucketProps

func TestBuildRpbStoreBucketReqCorrectlyViaBuilder(t *testing.T) {
	trueVal := true
	uint32val := uint32(9)

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
		t.FailNow()
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

	cmd.onSuccess(nil)
	if expected, actual := true, cmd.Successful(); expected != actual {
		t.Errorf("expected %v, actual %v", expected, actual)
	}

	if storeBucketPropsCommand, ok := cmd.(*StoreBucketPropsCommand); ok {
		if expected, actual := true, storeBucketPropsCommand.Success; expected != actual {
			t.Errorf("expected %v, actual %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *StoreBucketPropsCommand", ok, reflect.TypeOf(cmd))
	}
}
