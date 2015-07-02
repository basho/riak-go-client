package riak

// Ping Command and Builder

type PingCommandBuilder struct {
}

func (builder *PingCommandBuilder) Build() (Command, error) {
	return &PingCommand{}, nil
}

type PingCommand struct {
	Result bool
}

func (cmd *PingCommand) Name() string {
	return "Ping"
}

func (cmd *PingCommand) Success() bool {
	return cmd.Result == true
}

func (cmd *PingCommand) rpbData() ([]byte, error) {
	return rpbWrite(rpbCode_RpbPingReq, nil), nil
}

func (cmd *PingCommand) rpbRead(data []byte) (err error) {
	err = rpbValidateResp(data, rpbCode_RpbPingResp)
	if err == nil {
		cmd.Result = true
	} else {
		cmd.Result = false
	}
	return
}
