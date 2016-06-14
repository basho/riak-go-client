// +build timeseries

package riak

import (
	"fmt"
	"testing"
	"time"
)

const tsTimestamp = 1443806900
const tsQuery = `select * from %v where region = 'South Atlantic' and state = 'South Carolina' and (time > %v and time < %v)`
const tsTable = `WeatherByRegion`
const tsTableDefinition = `
	CREATE TABLE %s (
		region varchar not null,
		state varchar not null,
		time timestamp not null,
		weather varchar not null,
		temperature double,
		uv_index sint64,
		observed boolean not null,
		PRIMARY KEY((region, state, quantum(time, 15, 'm')), region, state, time)
	)`

// TsFetchRow
func TestTsFetchRowNotFound(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsFetchRowCommandBuilder()
	key := make([]TsCell, 3)

	key[0] = NewStringTsCell("South Atlantic")
	key[1] = NewStringTsCell("South Carolina")
	key[2] = NewTimestampTsCell(time.Now().Unix())

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithKey(key).WithTimeout(time.Second * 30).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*TsFetchRowCommand); ok {
		rsp := scmd.Response
		if rsp == nil {
			t.Errorf("expected non-nil Response")
		}

		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := true, scmd.Response.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsQuery
func TestTsDescribeTable(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsQueryCommandBuilder()

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithQuery("DESCRIBE " + tsTable).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}

	if scmd, ok := cmd.(*TsQueryCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 5, len(scmd.Response.Columns); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsQuery
func TestTsCreateTable(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsQueryCommandBuilder()

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	query := fmt.Sprintf(tsTableDefinition, fmt.Sprintf("%v%v", tsTable, time.Now().Unix()))
	cmd, err = sbuilder.WithQuery(query).Build()
	if err != nil {
		t.Log(query)
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Log(query)
		t.Fatal(err.Error())
	}

	if scmd, ok := cmd.(*TsQueryCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 0, len(scmd.Response.Columns); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsStoreRows
func TestTsStoreRow(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsStoreRowsCommandBuilder()
	row := make([]TsCell, 7)

	row[0] = NewStringTsCell("South Atlantic")
	row[1] = NewStringTsCell("South Carolina")
	row[2] = NewTimestampTsCell(tsTimestamp)
	row[3] = NewStringTsCell("hot")
	row[4] = NewDoubleTsCell(23.5)
	row[5] = NewSint64TsCell(10)
	row[6] = NewBooleanTsCell(true)

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithRows([][]TsCell{row}).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*TsStoreRowsCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := true, scmd.Response; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsStoreRows
func TestTsStoreRows(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsStoreRowsCommandBuilder()
	row := make([]TsCell, 7)

	row[0] = NewStringTsCell("South Atlantic")
	row[1] = NewStringTsCell("South Carolina")
	row[2] = NewTimestampTsCell(tsTimestamp - 3600)
	row[3] = NewStringTsCell("windy")
	row[4] = NewDoubleTsCell(19.8)
	row[5] = NewSint64TsCell(10)
	row[6] = NewBooleanTsCell(true)

	row2 := row
	row[2] = NewTimestampTsCell(tsTimestamp - 7200)
	row[3] = NewStringTsCell("cloudy")
	row[4] = NewDoubleTsCell(19.1)
	row[5] = NewSint64TsCell(15)
	row[6] = NewBooleanTsCell(false)

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithRows([][]TsCell{row, row2}).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*TsStoreRowsCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := true, scmd.Response; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsFetchRow
func TestTsFetchRow(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsFetchRowCommandBuilder()
	key := make([]TsCell, 3)

	key[0] = NewStringTsCell("South Atlantic")
	key[1] = NewStringTsCell("South Carolina")
	key[2] = NewTimestampTsCell(tsTimestamp)

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithKey(key).WithTimeout(time.Second * 30).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*TsFetchRowCommand); ok {
		rsp := scmd.Response
		if rsp == nil {
			t.Errorf("expected non-nil Response")
		}

		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := false, rsp.IsNotFound; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 7, len(rsp.Columns); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 7, len(rsp.Row); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		} else {
			t.Log(rsp.Row[0].cell, rsp.Row[4].cell)
			if expected, actual := "TIMESTAMP", rsp.Row[2].GetDataType(); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			} else {
				if expected, actual := tsTimestamp, rsp.Row[2].GetTimestampValue(); expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			}

			if expected, actual := "VARCHAR", rsp.Row[3].GetDataType(); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}

			if expected, actual := "DOUBLE", rsp.Row[4].GetDataType(); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}

			if expected, actual := "SINT64", rsp.Row[5].GetDataType(); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}

			if expected, actual := "BOOLEAN", rsp.Row[6].GetDataType(); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}
}

// TsQuery
func TestTsQuery(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsQueryCommandBuilder()
	upperBound := time.Unix(tsTimestamp, 0).Add(time.Second)
	lowerBound := time.Unix(tsTimestamp, 0).Add(-time.Second * 3601)
	query := fmt.Sprintf(tsQuery, tsTable, lowerBound.Unix(), upperBound.Unix())

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithQuery(query).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal("The errors: " + err.Error())
	}
	if scmd, ok := cmd.(*TsQueryCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := 7, len(scmd.Response.Columns); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// TsListKeys
func TestTsListKeys(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsListKeysCommandBuilder()

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithTimeout(time.Second * 30).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}

	if scmd, ok := cmd.(*TsListKeysCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := true, len(scmd.Response.Keys) > 0; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		t.Log(scmd.Response.Keys)
	} else {
		t.FailNow()
	}
}

// TsDeleteRow
func TestTsDeleteRow(t *testing.T) {
	var err error
	var cmd Command
	sbuilder := NewTsDeleteRowCommandBuilder()
	key := make([]TsCell, 3)

	key[0] = NewStringTsCell("South Atlantic")
	key[1] = NewStringTsCell("South Carolina")
	key[2] = NewTimestampTsCell(tsTimestamp)

	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	cmd, err = sbuilder.WithTable(tsTable).WithKey(key).WithTimeout(time.Second * 30).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*TsDeleteRowCommand); ok {
		if expected, actual := true, scmd.success; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}

		if expected, actual := true, scmd.Response; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
