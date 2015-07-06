package riak

import (
	"bytes"
	"testing"
)

func TestRpbWrite(t *testing.T) {
	expected := []byte{0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	data := []byte{2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := buildRiakMessage(1, data)
	if !bytes.Equal(expected, buf) {
		t.Errorf("expected %v, got %v", expected, buf)
	}
}

func TestRpbOldWrite(t *testing.T) {
	expected := []byte{0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	data := []byte{2, 3, 4, 5, 6, 7, 8, 9, 10}
	buf := rpbOldWrite(1, data)
	if !bytes.Equal(expected, buf) {
		t.Errorf("expected %v, got %v", expected, buf)
	}
}
