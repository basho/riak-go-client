package riak

import (
	"testing"
)

func TestDeleteFromSliceWhileIterating(t *testing.T) {
	s := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(s) != 10 {
		t.Errorf("expected 10 elements, got %v", len(s))
	}
	for i := 0; i < len(s); {
		e := s[i]
		// t.Log(i, "Processing:", e)
		if e%2 == 0 {
			l := len(s) - 1
			s[i], s[l], s = s[l], 0, s[:l]
		} else {
			i++
		}
	}
	if len(s) != 5 {
		t.Errorf("expected 5 elements, got %v", len(s))
	}
	/*
		for i, e := range s {
			t.Log(i, "Processed:", e)
		}
	*/
}
