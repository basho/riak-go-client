package riak

import (
	proto "github.com/golang/protobuf/proto"
)

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

func (cmd *PingCommand) getRequestCode() byte {
	return rpbCode_RpbPingReq
}

func (cmd *PingCommand) constructPbRequest() (msg proto.Message, err error) {
	return nil, nil
}

func (cmd *PingCommand) onSuccess(msg proto.Message) error {
	cmd.IsSuccess = true
	return nil
}

func (cmd *PingCommand) getExpectedResponseCode() byte {
	return rpbCode_RpbPingResp
}

func (cmd *PingCommand) getResponseProtobufMessage() proto.Message {
	return nil
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
