package riak

import (
	"reflect"
	"testing"

	"github.com/basho/riak-go-client/rpb/riak_ts"
)

func TestBuildTsGetReqCorrectlyViaBuilder(t *testing.T) {
	key := make([]TsCell, 3)

	key[0] = NewStringTsCell("Test Key Value")
	key[1] = NewSint64TsCell(1)
	key[2] = NewDoubleTsCell(0.1)

	builder := NewTsFetchRowCommandBuilder().
		WithTable("table_name").
		WithKey(key)

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, ok := cmd.(retryableCommand); !ok {
		t.Errorf("got %v, want cmd %s to implement retryableCommand", ok, reflect.TypeOf(cmd))
	}

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.FailNow()
	}

	if req, ok := protobuf.(*riak_ts.TsGetReq); ok {
		if expected, actual := "table_name", string(req.GetTable()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 3, len(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *riak_ts.TsGetReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestBuildTsDelReqCorrectlyViaBuilder(t *testing.T) {
	key := make([]TsCell, 3)

	key[0] = NewStringTsCell("Test Key Value")
	key[1] = NewSint64TsCell(1)
	key[2] = NewDoubleTsCell(0.1)

	builder := NewTsDeleteRowCommandBuilder().
		WithTable("table_name").
		WithKey(key)

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, ok := cmd.(retryableCommand); !ok {
		t.Errorf("got %v, want cmd %s to implement retryableCommand", ok, reflect.TypeOf(cmd))
	}

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}
	if protobuf == nil {
		t.FailNow()
	}

	if req, ok := protobuf.(*riak_ts.TsDelReq); ok {
		if expected, actual := "table_name", string(req.GetTable()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 3, len(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *riak_ts.TsDelReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestBuildTsPutReqCorrectlyViaBuilder(t *testing.T) {
	row := make([]TsCell, 5)

	row[0] = NewStringTsCell("Test Key Value")
	row[1] = NewSint64TsCell(1)
	row[2] = NewDoubleTsCell(0.1)
	row[3] = NewBooleanTsCell(true)
	row[4] = NewTimestampTsCell(1234567890)

	rows := make([][]TsCell, 1)
	rows[0] = row

	builder := NewTsStoreRowsCommandBuilder().
		WithTable("table_name").
		WithRows(rows)

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, ok := cmd.(retryableCommand); !ok {
		t.Errorf("got %v, want cmd %s to implement retryableCommand", ok, reflect.TypeOf(cmd))
	}

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		t.Fatal(err.Error())
	}

	if protobuf == nil {
		t.FailNow()
	}

	if req, ok := protobuf.(*riak_ts.TsPutReq); ok {
		if expected, actual := "table_name", string(req.GetTable()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 1, len(req.GetRows()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *riak_ts.TsPutReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestBuildTsQueryReqCorrectlyViaBuilder(t *testing.T) {

	builder := NewTsQueryCommandBuilder().
		WithQuery("DESCRIBE table_name")

	if builder.protobuf.GetStream() != false {
		t.Errorf("expected %v, got %v", nil, builder.protobuf.GetStream())
	}

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	builder.WithStreaming(true)
	if expected, actual := true, builder.protobuf.GetStream(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	cmd, err = builder.Build()
	if err == nil {
		t.Fatal("Expected an error, you cannot build the command with streaming true and callback = nil")
	}

	cb := func(rows [][]TsCell) error {
		// do stuff
		return nil
	}

	builder.WithCallback(cb)

	cmd, err = builder.Build()
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

	if req, ok := protobuf.(*riak_ts.TsQueryReq); ok {
		if expected, actual := "DESCRIBE table_name", string(req.GetQuery().GetBase()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetStream(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *riak_ts.TsQueryReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestBuildTsListKeysReqCorrectlyViaBuilder(t *testing.T) {
	builder := NewTsListKeysCommandBuilder().
		WithTable("table_name")

	if expected, actual := false, builder.streaming; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	cmd, err := builder.Build()
	if err != nil {
		t.Fatal(err.Error())
	}

	builder.WithStreaming(true)
	if expected, actual := true, builder.streaming; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	cmd, err = builder.Build()
	if err == nil {
		t.Fatal("Expected an error, you cannot build the command with streaming true and callback = nil")
	}

	cb := func(keys []string) error {
		// do stuff
		return nil
	}

	builder.WithCallback(cb)
	cmd, err = builder.Build()
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

	if req, ok := protobuf.(*riak_ts.TsListKeysReq); ok {
		if expected, actual := "table_name", string(req.GetTable()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *riak_ts.TsListKeysReq", ok, reflect.TypeOf(protobuf))
	}
}

func TestNewTsCells(t *testing.T) {
	cells := make([]TsCell, 5)
	cells[0] = NewStringTsCell("Test Key Value")
	cells[1] = NewSint64TsCell(1)
	cells[2] = NewDoubleTsCell(0.1)
	cells[3] = NewBooleanTsCell(true)
	cells[4] = NewTimestampTsCell(1234567890)

	if expected, actual := "VARCHAR", cells[0].GetDataType(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := "SINT64", cells[1].GetDataType(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := "DOUBLE", cells[2].GetDataType(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := "BOOLEAN", cells[3].GetDataType(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := "TIMESTAMP", cells[4].GetDataType(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
