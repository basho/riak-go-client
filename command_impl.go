package riak

type CommandImpl struct {
	Error          error
	success        bool
	remainingTries byte
	lastNode       *Node
}

func (cmd *CommandImpl) Success() bool {
	return cmd.success == true
}

func (cmd *CommandImpl) onError(err error) {
	cmd.success = false
	// NB: only set error to the *last* error (retries)
	if !cmd.hasRemainingTries() {
		cmd.Error = err
	}
}

func (cmd *CommandImpl) setRemainingTries(tries byte) {
	cmd.remainingTries = tries
}

func (cmd *CommandImpl) decrementRemainingTries() {
	cmd.remainingTries--
	logDebug("[CommandImpl]", "remainingTries: %d", cmd.remainingTries)
}

func (cmd *CommandImpl) hasRemainingTries() bool {
	return cmd.remainingTries > 0
}

func (cmd *CommandImpl) setLastNode(lastNode *Node) {
	if lastNode == nil {
		panic("[CommandImpl] nil last node")
	}
	cmd.lastNode = lastNode
}

func (cmd *CommandImpl) getLastNode() *Node {
	return cmd.lastNode
}
