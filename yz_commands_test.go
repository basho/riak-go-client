package riak

import (
	rpbRiakYZ "github.com/basho-labs/riak-go-client/rpb/riak_yokozuna"
	"reflect"
	"testing"
	"time"
)

// StoreIndex
// RpbYokozunaIndexPutReq

func TestBuildRpbYokozunaIndexPutReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewStoreIndexCommandBuilder().
		WithIndexName("indexName").
		WithSchemaName("indexName_schema").
		WithNVal(5).
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
	if req, ok := protobuf.(*rpbRiakYZ.RpbYokozunaIndexPutReq); ok {
		index := req.Index
		if expected, actual := "indexName", string(index.GetName()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "indexName_schema", string(index.GetSchema()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(5), index.GetNVal(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		validateTimeout(t, time.Second*20, req.GetTimeout())
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbYokozunaIndexPutReq", ok, reflect.TypeOf(protobuf))
	}
}

// FetchIndex
// RpbYokozunaIndexGetReq
// RpbYokozunaIndexGetResp

func TestBuildRpbYokozunaIndexGetReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewFetchIndexCommandBuilder().
		WithIndexName("indexName")
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
	if req, ok := protobuf.(*rpbRiakYZ.RpbYokozunaIndexGetReq); ok {
		if expected, actual := "indexName", string(req.GetName()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbYokozunaIndexGetReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbYokozunaIndexGetRespCorrectly(t *testing.T) {
	var nval uint32 = 9
	indexes := make([]*rpbRiakYZ.RpbYokozunaIndex, 1)
	indexes[0] = &rpbRiakYZ.RpbYokozunaIndex{
		Name:   []byte("indexName"),
		Schema: []byte("_yz_default"),
		NVal:   &nval,
	}
	resp := &rpbRiakYZ.RpbYokozunaIndexGetResp{Index: indexes}
	builder := NewFetchIndexCommandBuilder().WithIndexName("indexName")
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cmd.onSuccess(resp); err != nil {
		t.Fatal(err.Error())
	} else {
		if fcmd, ok := cmd.(*FetchIndexCommand); ok {
			if expected, actual := 1, len(fcmd.Response); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			idx := fcmd.Response[0]
			if expected, actual := "indexName", idx.Name; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "_yz_default", idx.Schema; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := nval, idx.NVal; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		} else {
			t.Errorf("ok: %v - could not convert %v to FetchIndexCommand", ok, reflect.TypeOf(cmd))
		}
	}
}

// DeleteIndex
// RpbYokozunaIndexDeleteReq

func TestBuildRpbYokozunaIndexDeleteReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewDeleteIndexCommandBuilder().
		WithIndexName("indexName")
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
	if req, ok := protobuf.(*rpbRiakYZ.RpbYokozunaIndexDeleteReq); ok {
		if expected, actual := "indexName", string(req.GetName()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbYokozunaIndexDeleteReq", ok, reflect.TypeOf(protobuf))
	}
}
