package riak

import (
	"time"

	"github.com/basho/backoff"
)

// NodeManager enforces the structure needed to if going to implement your own NodeManager
type NodeManager interface {
	Init(nodes []*Node) error
	ExecuteOnNode(command Command, previousNode *Node) (bool, error)
	Stop()
}

var ErrDefaultNodeManagerRequiresNode = newClientError("Must pass at least one node to default node manager")

type defaultNodeManager struct {
	qsz uint16
	q   *queue
	b   *backoff.Backoff
}

func (nm *defaultNodeManager) Init(nodes []*Node) error {
	if nodes == nil {
		panic("[defaultNodeManager] nil nodes argument")
	}
	if len(nodes) == 0 || nodes[0] == nil {
		return ErrDefaultNodeManagerRequiresNode
	}
	nm.qsz = uint16(len(nodes))
	nm.q = newQueue(nm.qsz)
	nm.b = &backoff.Backoff{
		Min:    10 * time.Millisecond,
		Max:    100 * time.Millisecond,
		Jitter: true,
	}
	for _, n := range nodes {
		nm.q.enqueue(n)
	}
	return nil
}

// ExecuteOnNode selects a Node from the pool and executes the provided Command on that Node. The
// defaultNodeManager uses a simple round robin approach to distributing load
func (nm *defaultNodeManager) ExecuteOnNode(command Command, previous *Node) (bool, error) {
	var err error
	var executed bool = false

	i := uint16(0)
	var f = func(v interface{}) (bool, bool) {
		if v == nil {
			// pool is empty now, re-try
			d := nm.b.Duration()
			logDebug("[DefaultNodeManager]", "pool is empty, sleeping for '%v'", d)
			time.Sleep(d)
			return false, false
		}
		i++
		node := v.(*Node)

		// don't try the same node twice in a row if we have multiple nodes
		if nm.qsz > 1 && previous != nil && previous == node {
			return false, true
		}

		if executed, err = node.execute(command); executed == true {
			logDebug("[DefaultNodeManager]", "executed '%s' on node '%s', err '%v'", command.Name(), node, err)
			return true, true
		}

		if i == nm.qsz {
			logDebug("[DefaultNodeManager]", "tried all nodes to execute '%s'", command.Name())
			return true, true
		}

		return false, true
	}

	if ierr := nm.q.iterate(f); ierr != nil {
		logErr("[DefaultNodeManager] iteration error", ierr)
	}

	return executed, err
}

func (nm *defaultNodeManager) Stop() {
	nm.q.destroy()
}
