package riak

type commandImpl struct {
	error          error
	success        bool
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
