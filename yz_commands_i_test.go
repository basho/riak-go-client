// +build integration

package riak

import (
	"testing"
	"time"
)

func init() {
	integrationTestsBuildCluster()
}

// FetchIndex
// StoreIndex

func TestStoreFetchAndDeleteAYokozunaIndex(t *testing.T) {
	var err error
	var cmd Command
	indexName := "indexName"
	sbuilder := NewStoreIndexCommandBuilder()
	cmd, err = sbuilder.WithIndexName(indexName).WithTimeout(time.Second * 10).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if scmd, ok := cmd.(*StoreIndexCommand); ok {
		if expected, actual := true, scmd.Response; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}

	fbuilder := NewFetchIndexCommandBuilder()
	cmd, err = fbuilder.WithIndexName(indexName).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fcmd, ok := cmd.(*FetchIndexCommand); ok {
		if fcmd.Response == nil {
			t.Errorf("expected non-nil Response")
		}
		idx := fcmd.Response[0]
		if expected, actual := indexName, idx.Name; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "_yz_default", idx.Schema; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(3), idx.NVal; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}

	dbuilder := NewDeleteIndexCommandBuilder()
	cmd, err = dbuilder.WithIndexName(indexName).Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if dcmd, ok := cmd.(*DeleteIndexCommand); ok {
		if expected, actual := true, dcmd.Response; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}
