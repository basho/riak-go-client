// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

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
	stateVal  state
	stateDesc []string
}

func newStateData(desc ...string) *stateData {
	sd := &stateData{
		stateDesc: desc,
	}
	return sd
}

func (s *stateData) String() string {
	stateIdx := int(s.stateVal)
	if len(s.stateDesc) > stateIdx {
		return s.stateDesc[stateIdx]
	} else {
		return fmt.Sprintf("STATE_%v", stateIdx)
	}
}

func (s *stateData) setStateDesc(desc ...string) {
	s.stateDesc = desc
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

var setStateFunc = func(s *stateData, st state) {
	s.Lock()
	defer s.Unlock()
	s.stateVal = st
}

func (s *stateData) setState(st state) {
	setStateFunc(s, st)
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
