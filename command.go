package riak

import (
	"errors"
)

type CommandBuilder interface {
	Build() Command
}

type PingCommandBuilder struct {
}

func (builder *PingCommandBuilder) Build() Command {
	return &PingCommand{}
}

type Command interface {
	Name() string
	Success() bool
	rpbData() []byte
	rpbRead(data []byte) error
}

type PingCommand struct {
	Result bool
}

var ErrZeroLength error = errors.New("[Command] 0 byte data response")

func (cmd *PingCommand) Name() string {
	return "Ping"
}

func (cmd *PingCommand) Success() bool {
	return cmd.Result == true
}

func (cmd *PingCommand) rpbData() []byte {
	// PingReq: 1
	return rpbWrite(1, nil)
}

func (cmd *PingCommand) rpbRead(data []byte) (err error) {
	if len(data) == 0 {
		err = ErrZeroLength
		return
	}

	if err = rpbEnsureCode(rpbCode_RpbPingResp, data[0]); err != nil {
		return
	}

	if err == nil {
		cmd.Result = true
	} else {
		cmd.Result = false
	}

	return
}
