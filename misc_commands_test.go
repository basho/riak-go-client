package riak

import (
	rpbRiak "github.com/basho-labs/riak-go-client/rpb/riak"
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
		/*
			response.precommit[0].mod "module_name"
			response.precommit[0].fun "function_name"
			response.postcommit[0].mod "module_name"
			response.postcommit[0].fun "function_name"
			response.chashKeyfun.mod "module_name"
			response.chashKeyfun.fun "function_name"

			response.linkFun.mod "module_name"
			response.linkFun.fun "function_name"
		*/
	} else {
		t.Errorf("ok: %v - could not convert %v to *FetchBucketPropsCommand", ok, reflect.TypeOf(cmd))
	}
}
