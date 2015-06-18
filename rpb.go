package riak

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func rpbEnsureCode(expected byte, actual byte) (err error) {
	if expected != actual {
		err = fmt.Errorf("expected response code %d, got: %d", expected, actual)
	}
	return
}

func rpbWrite(code byte, data []byte) []byte {
	buf := new(bytes.Buffer)
	// write total message length, including one byte for msg code
	binary.Write(buf, binary.BigEndian, uint32(len(data)+1))
	// write the message code
	binary.Write(buf, binary.BigEndian, byte(code))
	// write the protobuf data
	buf.Write(data)
	return buf.Bytes()
}
