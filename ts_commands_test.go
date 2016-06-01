package riak

import (
	"reflect"
	"testing"

	"github.com/basho/riak-go-client/rpb/riak_ts"
	"github.com/golang/protobuf/proto"
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
	validateTsGetReq(t, protobuf)
}

func TestBuildTsPutReqCorrectlyViaBuilder(t *testing.T) {
	row := make([]TsCell, 3)

	row[0] = NewStringTsCell("Test Key Value")
	row[1] = NewSint64TsCell(1)
	row[2] = NewDoubleTsCell(0.1)
	row[3] = NewBooleanTsCell(true)
	row[4] = NewTimestampTsCell(1234567890)

	rows := make([][]TsCell, 0)
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
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbGetReq", ok, reflect.TypeOf(protobuf))
	}
}

func validateTsGetReq(t *testing.T, protobuf proto.Message) {
	if req, ok := protobuf.(*riak_ts.TsGetReq); ok {
		if expected, actual := "table_name", string(req.GetTable()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 3, len(req.GetKey()); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Errorf("ok: %v - could not convert %v to *rpbRiakKV.RpbGetReq", ok, reflect.TypeOf(protobuf))
	}
}
