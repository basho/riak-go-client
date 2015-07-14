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
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbGetReq", ok, reflect.TypeOf(protobuf))
	}
}
