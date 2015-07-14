package riak

import (
	rpbRiakSCH "github.com/basho-labs/riak-go-client/rpb/riak_search"
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

// StoreSchema
// RpbYokozunaSchemaPutReq

func TestBuildRpbYokozunaSchemaPutReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewStoreSchemaCommandBuilder().
		WithSchemaName("schemaName").
		WithSchema("schema_xml")
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
	if req, ok := protobuf.(*rpbRiakYZ.RpbYokozunaSchemaPutReq); ok {
		schema := req.Schema
		if expected, actual := "schemaName", string(schema.GetName()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "schema_xml", string(schema.GetContent()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbYokozunaSchemaPutReq", ok, reflect.TypeOf(protobuf))
	}
}

// FetchSchema
// RpbYokozunaSchemaGetReq
// RpbYokozunaSchemaGetResp

func TestBuildRpbYokozunaSchemaGetReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewFetchSchemaCommandBuilder().
		WithSchemaName("schemaName")
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
	if req, ok := protobuf.(*rpbRiakYZ.RpbYokozunaSchemaGetReq); ok {
		if expected, actual := "schemaName", string(req.GetName()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbYokozunaSchemaGetReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestParseRpbYokozunaSchemaGetRespCorrectly(t *testing.T) {
	schema := &rpbRiakYZ.RpbYokozunaSchema{
		Name:    []byte("schemaName"),
		Content: []byte("schema_xml"),
	}
	resp := &rpbRiakYZ.RpbYokozunaSchemaGetResp{Schema: schema}
	builder := NewFetchSchemaCommandBuilder().WithSchemaName("schemaName")
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cmd.onSuccess(resp); err != nil {
		t.Fatal(err.Error())
	} else {
		if fcmd, ok := cmd.(*FetchSchemaCommand); ok {
			schema := fcmd.Response
			if expected, actual := "schemaName", schema.Name; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "schema_xml", schema.Content; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		} else {
			t.Errorf("ok: %v - could not convert %v to FetchSchemaCommand", ok, reflect.TypeOf(cmd))
		}
	}
}

// Search
// RpbSearchQueryReq
// RpbSearchQueryResp

func TestBuildRpbSearchQueryReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewSearchCommandBuilder().
		WithIndexName("indexName").
		WithQuery("*:*").
		WithNumRows(128).
		WithStart(2).
		WithSortField("sortField").
		WithFilterQuery("filterQuery").
		WithDefaultField("defaultField").
		WithDefaultOperation("and").
		WithReturnFields("field1", "field2").
		WithPresort("score")
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
	if req, ok := protobuf.(*rpbRiakSCH.RpbSearchQueryReq); ok {
		if expected, actual := "indexName", string(req.GetIndex()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "*:*", string(req.GetQ()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(128), req.GetRows(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetStart(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "sortField", string(req.GetSort()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "filterQuery", string(req.GetFilter()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "defaultField", string(req.GetDf()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "and", string(req.GetOp()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		rf := req.GetFl()
		if expected, actual := 2, len(rf); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "field1", string(rf[0]); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "field2", string(rf[1]); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "score", string(req.GetPresort()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbSearchQueryReq", ok, reflect.TypeOf(protobuf))
	}
}

/*
func TestParseRpbYokozunaSchemaGetRespCorrectly(t *testing.T) {
	schema := &rpbRiakYZ.RpbYokozunaSchema{
		Name:    []byte("schemaName"),
		Content: []byte("schema_xml"),
	}
	resp := &rpbRiakYZ.RpbYokozunaSchemaGetResp{Schema: schema}
	builder := NewFetchSchemaCommandBuilder().WithSchemaName("schemaName")
	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cmd.onSuccess(resp); err != nil {
		t.Fatal(err.Error())
	} else {
		if fcmd, ok := cmd.(*FetchSchemaCommand); ok {
			schema := fcmd.Response
			if expected, actual := "schemaName", schema.Name; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "schema_xml", schema.Content; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		} else {
			t.Errorf("ok: %v - could not convert %v to FetchSchemaCommand", ok, reflect.TypeOf(cmd))
		}
	}
}
*/
