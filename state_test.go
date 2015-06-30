package riak

import (
	"testing"
)

type testStatefulData struct {
	stateData
}

const (
	STATE_ONE state = iota
	STATE_TWO
	STATE_THREE

	OTHER_STATE_ONE
	OTHER_STATE_TWO
	OTHER_STATE_THREE
)

func TestStateConsts(t *testing.T) {
	if s1, s2 := STATE_ONE, OTHER_STATE_ONE; s1 == s2 {
		t.Errorf("whoops, %v equals %v", s1, s2)
	}
}

func TestStateful(t *testing.T) {
	data := &testStatefulData{}

	data.setState(STATE_TWO)

	if expected, actual := true, data.isCurrentState(STATE_TWO); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := false, data.isCurrentState(STATE_ONE); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
