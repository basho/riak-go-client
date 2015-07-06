package riak

import (
	"testing"
)

var data []byte

func BenchmarkRpbOldWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data = rpbOldWrite(byte((i%255)+1), randomBytes)
	}
}

func BenchmarkRpbWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data = buildRiakMessage(byte((i%255)+1), randomBytes)
	}
}
