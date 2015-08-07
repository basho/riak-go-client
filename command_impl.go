package riak

type CommandImpl struct {
	Error   error
	Success bool
}

func (cmd *CommandImpl) Successful() bool {
	return cmd.Success == true
}

func (cmd *CommandImpl) onError(err error) {
	cmd.Error = err
	cmd.Success = false
}
