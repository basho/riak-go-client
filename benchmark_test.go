// +build integration

package riak

import (
	"strconv"
	"testing"
)

func BenchmarkPuttingManyObjects(b *testing.B) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			b.Error(err)
		}
	}()

	for i := 0; i < b.N; i++ {
		obj := getBasicObject()
		obj.Value = randomBytes

		store, err := NewStoreValueCommandBuilder().
			WithBucket("memprofile").
			WithKey(strconv.Itoa(i)).
			WithContent(obj).
			Build()
		if err != nil {
			b.Fatal(err)
		}
		if err := cluster.Execute(store); err != nil {
			b.Fatal(err)
		}
	}
}
