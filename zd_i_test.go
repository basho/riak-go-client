// Copyright 2015-present Basho Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build zendesk

package riak

import (
	"fmt"
	"reflect"
	"testing"
)

// MapReduceCommand
func TestMapReduceZenDesk14385(t *testing.T) {
	EnableDebugLogging = true
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	bucket := fmt.Sprintf("%s_erlang_mr", testBucketName)
	storeData(t, cluster, bucket)

	queryFmt := "{\"inputs\":[[\"%s\",\"p0\"],[\"%s\",\"p1\"],[\"%s\",\"p2\"]],\"query\":[{\"map\":{\"language\":\"erlang\",\"module\":\"mapreduce_14385\",\"function\":\"map\"}},{\"reduce\":{\"language\":\"erlang\",\"module\":\"mapreduce_14385\",\"function\":\"reduce\"}}]}"
	query := fmt.Sprintf(queryFmt, bucket, bucket, bucket)

	if cmd, err := NewMapReduceCommandBuilder().WithQuery(query).Build(); err == nil {
		if err = cluster.Execute(cmd); err != nil {
			t.Fatal(err.Error())
		}
		if cerr := cmd.Error(); cerr != nil {
			t.Fatal(cerr)
		}
		if mr, ok := cmd.(*MapReduceCommand); ok {
			if mr.Response == nil || len(mr.Response) == 0 {
				t.Error("expected non-nil and non-empty response")
			} else {
				t.Logf("mapreduce response: %v", mr.Response[0])
				t.Logf("mapreduce response as string: %s", mr.Response[0])
			}
		} else {
			t.Errorf("Could not convert %v to *MapReduceQueryCommand", ok, reflect.TypeOf(cmd))
		}
	} else {
		t.Fatal(err.Error())
	}
}
