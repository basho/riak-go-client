package riak

import (
	"fmt"
	"sync/atomic"
)

var c uint64 = 0

type commandImpl struct {
	error          error
	success        bool
	name           string
	remainingTries byte
	lastNode       *Node
}

func (cmd *commandImpl) Success() bool {
	return cmd.success == true
}

func (cmd *commandImpl) Error() error {
	return cmd.error
}

func (cmd *commandImpl) onError(err error) {
	cmd.success = false
	// NB: only set error to the *last* error (retries)
	if !cmd.hasRemainingTries() {
		cmd.error = err
	}
}

func (cmd *commandImpl) getName(n string) string {
	if n == "" {
		panic("getName: n must not be empty")
	}
	if cmd.name == "" {
		if EnableDebugLogging == true {
			cmd.name = fmt.Sprintf("%s-%v", n, atomic.AddUint64(&c, 1))
		} else {
			cmd.name = n
		}
	}
	return cmd.name
}

func (cmd *commandImpl) setRemainingTries(tries byte) {
	cmd.remainingTries = tries
}

func (cmd *commandImpl) decrementRemainingTries() {
	cmd.remainingTries--
	logDebug("[commandImpl]", "remainingTries: %d", cmd.remainingTries)
}

func (cmd *commandImpl) hasRemainingTries() bool {
	return cmd.remainingTries > 0
}

func (cmd *commandImpl) setLastNode(lastNode *Node) {
	if lastNode == nil {
		panic("[commandImpl] nil last node")
	}
	cmd.lastNode = lastNode
}

func (cmd *commandImpl) getLastNode() *Node {
	return cmd.lastNode
}
