package riak

import (
	"errors"
)

type Command interface {
	rpbData() []byte
	rpbRead(data []byte) error
}

type PingCommand struct {
	Result bool
}

var ErrZeroLength error = errors.New("response was only 0 bytes long")

func (cmd *PingCommand) rpbData() []byte {
	// PingReq: 1
	return rpbWrite(1, nil)
}

func (cmd *PingCommand) rpbRead(data []byte) (err error) {
	if len(data) == 0 {
		err = ErrZeroLength
		return
	}

	// PingResp: 2
	if err = rpbEnsureCode(2, data[0]); err != nil {
		return
	}

	if err == nil {
		cmd.Result = true
	} else {
		cmd.Result = false
	}

	return
}
