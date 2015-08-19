package riak

import (
	"fmt"
	"sync"
)

type state byte

type stateful interface {
	fmt.Stringer
	setStateDesc(desc ...string)
	isCurrentState(st state) (rv bool)
	isStateLessThan(st state) (rv bool)
	setState(st state)
	getState() (st state)
	stateCheck(allowed ...state) (err error)
}

type stateData struct {
	sync.RWMutex
	stateVal     state
	stateDesc    []string
	setStateFunc func(sd *stateData, st state)
}

var defaultSetStateFunc = func(sd *stateData, st state) {
	sd.stateVal = st
}

func (s *stateData) initStateData(desc ...string) {
	s.stateDesc = desc
	s.setStateFunc = defaultSetStateFunc
}

func (s *stateData) String() string {
	stateIdx := int(s.stateVal)
	if len(s.stateDesc) > stateIdx {
		return s.stateDesc[stateIdx]
	} else {
		return fmt.Sprintf("STATE_%v", stateIdx)
	}
}

func (s *stateData) isCurrentState(st state) (rv bool) {
	rv = false
	s.RLock()
	defer s.RUnlock()
	rv = s.stateVal == st
	return
}

func (s *stateData) isStateLessThan(st state) (rv bool) {
	rv = false
	s.RLock()
	defer s.RUnlock()
	rv = s.stateVal < st
	return
}

func (s *stateData) getState() (st state) {
	s.RLock()
	defer s.RUnlock()
	st = s.stateVal
	return
}

func (s *stateData) setState(st state) {
	s.Lock()
	defer s.Unlock()
	s.setStateFunc(s, st)
}

func (s *stateData) stateCheck(allowed ...state) (err error) {
	s.RLock()
	defer s.RUnlock()
	stateAllowed := false
	for _, st := range allowed {
		if s.stateVal == st {
			stateAllowed = true
			break
		}
	}
	if !stateAllowed {
		err = fmt.Errorf("Illegal State - required %v: current: %v", allowed, s.stateVal)
	}
	return
}
