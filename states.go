package riak

import (
	"fmt"
	"sync"
)

type state byte

type stateful interface {
	fmt.Stringer
	isCurrentState(st state) (rv bool)
	setState(st state)
	getState() (st state)
	stateCheck(allowed ...state) (err error)
}

type stateData struct {
	sync.RWMutex
	stateVal  state
	stateDesc []string
}

func NewStateData(desc ...string) *stateData {
	return &stateData{
		stateDesc: desc,
	}
}

func (s *stateData) String() string {
	stateIdx := int(s.stateVal)
	if len(s.stateDesc) > stateIdx {
		return s.stateDesc[stateIdx]
	} else {
		return "UNKNOWN_STATE"
	}
}

func (s *stateData) isCurrentState(st state) (rv bool) {
	rv = false
	s.RLock()
	defer s.RUnlock()
	rv = s.stateVal == st
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
	s.stateVal = st
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

// Cluster states

type clusterState byte

const (
	CLUSTER_ERROR clusterState = iota
	CLUSTER_CREATED
	CLUSTER_RUNNING
	CLUSTER_QUEUING
	CLUSTER_SHUTTING_DOWN
	CLUSTER_SHUTDOWN
)

func (v clusterState) String() (rv string) {
	switch v {
	case CLUSTER_CREATED:
		rv = "CLUSTER_CREATED"
	case CLUSTER_RUNNING:
		rv = "CLUSTER_RUNNING"
	case CLUSTER_QUEUING:
		rv = "CLUSTER_QUEUING"
	case CLUSTER_SHUTTING_DOWN:
		rv = "CLUSTER_SHUTTING_DOWN"
	case CLUSTER_SHUTDOWN:
		rv = "CLUSTER_SHUTDOWN"
	}
	return
}

// Node states

type nodeState byte

const (
	NODE_ERROR nodeState = iota
	NODE_CREATED
	NODE_RUNNING
	NODE_HEALTH_CHECKING
	NODE_SHUTTING_DOWN
	NODE_SHUTDOWN
)

func (v nodeState) String() (rv string) {
	switch v {
	case NODE_CREATED:
		rv = "NODE_CREATED"
	case NODE_RUNNING:
		rv = "NODE_RUNNING"
	case NODE_HEALTH_CHECKING:
		rv = "NODE_HEALTH_CHECKING"
	case NODE_SHUTTING_DOWN:
		rv = "NODE_SHUTTING_DOWN"
	case NODE_SHUTDOWN:
		rv = "NODE_SHUTDOWN"
	}
	return
}
