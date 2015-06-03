package riak

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	code := data[0] // rpb code
	if code != 2 {
		err = errors.New(fmt.Sprintf("expected response code 2, got: %d", code))
	}

	if err == nil {
		cmd.Result = true
	} else {
		cmd.Result = false
	}

	return
}

// TODO: ensure this is the fastest way to write data to the buffer
func rpbWrite(code byte, data []byte) []byte {
	ml := new(bytes.Buffer)
	binary.Write(ml, binary.BigEndian, int32(len(data)+1)) // +1 for msg code
	mc := new(bytes.Buffer)
	binary.Write(mc, binary.BigEndian, int8(code))
	buf := []byte(ml.Bytes())
	buf = append(buf, mc.Bytes()...)
	buf = append(buf, data...)
	return buf
}
