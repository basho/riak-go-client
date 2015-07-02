package riak

// Ping Command and Builder

type PingCommandBuilder struct {
}

func (builder *PingCommandBuilder) Build() (Command, error) {
	return &PingCommand{}, nil
}

type PingCommand struct {
	CommandImpl
}

func (cmd *PingCommand) Name() string {
	return "Ping"
}

func (cmd *PingCommand) rpbData() ([]byte, error) {
	return rpbWrite(rpbCode_RpbPingReq, nil), nil
}

func (cmd *PingCommand) rpbRead(data []byte) (err error) {
	// TODO take onError into account
	err = rpbValidateResp(data, rpbCode_RpbPingResp)
	if err == nil {
		cmd.IsSuccess = true
	} else {
		cmd.IsSuccess = false
	}
	return
}
