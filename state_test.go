package riak

import (
	"testing"
)

type testStateData struct {
	stateData
}

const (
	STATE_ONE state = iota
	STATE_TWO
	STATE_THREE
	STATE_FOUR

	OTHER_STATE_ONE
	OTHER_STATE_TWO
	OTHER_STATE_THREE
)

func TestStateConsts(t *testing.T) {
	data1 := &testStateData{}
	data1.setState(STATE_ONE)

	data2 := &testStateData{}
	data2.setState(OTHER_STATE_ONE)

	if s1, s2 := data1.getState(), data2.getState(); s1 == s2 {
		t.Errorf("whoops, %v equals %v", s1, s2)
	}
}

func TestStateData(t *testing.T) {
	data := &testStateData{}
	data.setState(STATE_TWO)

	if expected, actual := true, data.isCurrentState(STATE_TWO); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	if expected, actual := false, data.isCurrentState(STATE_ONE); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestAllowedState(t *testing.T) {
	data := &testStateData{}
	data.setState(STATE_TWO)

	if err := data.stateCheck(STATE_ONE, STATE_THREE); err == nil {
		t.Errorf("expected non-nil error, got %v", err)
	} else {
		t.Logf("stateCheck err: %v", err)
	}
}

func TestStateDesc(t *testing.T) {
	data := NewStateData("STATE_ONE", "STATE_TWO", "STATE_THREE")

	data.setState(STATE_ONE)
	if expected, actual := "STATE_ONE", data.String(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	data.setState(STATE_TWO)
	if expected, actual := "STATE_TWO", data.String(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}

	data.setState(STATE_THREE)
	if expected, actual := "STATE_THREE", data.String(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestStateDescUnknown(t *testing.T) {
	data := NewStateData("STATE_ONE", "STATE_TWO", "STATE_THREE")
	data.setState(STATE_FOUR)

	if expected, actual := "UNKNOWN_STATE", data.String(); expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
