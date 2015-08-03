// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

type NodeManager interface {
	ExecuteOnNode(nodes []*Node, command Command, previous *Node) (executed bool, err error)
}

type defaultNodeManager struct {
	nodeIndex uint16
}

func (nm *defaultNodeManager) ExecuteOnNode(nodes []*Node, command Command, previous *Node) (executed bool, err error) {
	executed = false
	startingIndex := nm.nodeIndex

	for {
		node := nodes[nm.nodeIndex]

		nm.nodeIndex++

		if int(nm.nodeIndex) == len(nodes) {
			nm.nodeIndex = 0
		}

		// don't try the same node twice in a row if we have multiple nodes
		if len(nodes) > 1 && previous != nil && previous == node {
			continue
		}

		if executed, err = node.Execute(command); err != nil {
			executed = false
			logErr(err)
		} else {
			executed = true
			break
		}

		if nm.nodeIndex == startingIndex {
			break
		}
	}

	return
}
