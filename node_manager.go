package riak

import (
	"sync"
)

// NodeManager enforces the structure needed to if going to implement your own NodeManager
type NodeManager interface {
	ExecuteOnNode(nodes []*Node, command Command, previousNode *Node) (bool, error)
}

var ErrDefaultNodeManagerRequiresNode = newClientError("Must pass at least one node to default node manager", nil)

type defaultNodeManager struct {
	nodeIndex int
	sync.RWMutex
}

// ExecuteOnNode selects a Node from the pool and executes the provided Command on that Node. The
// defaultNodeManager uses a simple round robin approach to distributing load
func (nm *defaultNodeManager) ExecuteOnNode(nodes []*Node, command Command, previous *Node) (bool, error) {
	if nodes == nil {
		panic("[defaultNodeManager] nil nodes argument")
	}
	if len(nodes) == 0 || nodes[0] == nil {
		return false, ErrDefaultNodeManagerRequiresNode
	}

	var err error
	executed := false

	nm.RLock()
	startingIndex := nm.nodeIndex
	nm.RUnlock()

	for {
		nm.Lock()
		if nm.nodeIndex >= len(nodes) {
			nm.nodeIndex = 0
		}
		node := nodes[nm.nodeIndex]
		nm.nodeIndex++
		nm.Unlock()

		// don't try the same node twice in a row if we have multiple nodes
		if len(nodes) > 1 && previous != nil && previous == node {
			continue
		}

		executed, err = node.execute(command)
		if executed == true {
			logDebug("[DefaultNodeManager]", "executed '%s' on node '%s', err '%v'", command.Name(), node, err)
			break
		}

		nm.RLock()
		if startingIndex == nm.nodeIndex {
			nm.RUnlock()
			// logDebug("[DefaultNodeManager]", "startingIndex %d nm.nodeIndex %d", startingIndex, nm.nodeIndex)
			break
		}
		nm.RUnlock()
	}

	return executed, err
}
