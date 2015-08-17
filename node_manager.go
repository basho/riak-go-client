package riak

import "sync"

// NodeManager enforces the structure needed to if going to implement your own NodeManager
type NodeManager interface {
	Init(nodes []*Node) error
	ExecuteOnNode(command Command, previousNode *Node) (bool, error)
}

var ErrDefaultNodeManagerRequiresNode = newClientError("Must pass at least one node to default node manager")

type defaultNodeManager struct {
	nodes     []*Node
	nodeIndex uint16
	mtx       sync.Mutex
}

func (nm *defaultNodeManager) Init(nodes []*Node) error {
	if nodes == nil {
		panic("[defaultNodeManager] nil nodes argument")
	}
	if len(nodes) == 0 || nodes[0] == nil {
		return ErrDefaultNodeManagerRequiresNode 
	}
	nm.nodes = nodes
	return nil
}

// ExecuteOnNode selects a Node from the pool and executes the provided Command on that Node. The
// defaultNodeManager uses a simple round robin approach to distributing load
func (nm *defaultNodeManager) ExecuteOnNode(command Command, previous *Node) (executed bool, err error) {
	executed = false
	startingIndex := nm.nodeIndex

	for {
		nm.mtx.Lock()
		node := nm.nodes[nm.nodeIndex]
		nm.nodeIndex++
		if int(nm.nodeIndex) == len(nm.nodes) {
			nm.nodeIndex = 0
		}
		nm.mtx.Unlock()

		// don't try the same node twice in a row if we have multiple nodes
		if len(nm.nodes) > 1 && previous != nil && previous == node {
			continue
		}

		if executed, err = node.execute(command); executed == true {
			logDebug("[DefaultNodeManager]", "executed '%s' on node '%s', err '%v'", command.Name(), node, err)
			break
		}

		if nm.nodeIndex == startingIndex {
			break
		}
	}

	return
}
