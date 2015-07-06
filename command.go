package riak

import (
	"bytes"
	"encoding/binary"
	"fmt"
	proto "github.com/golang/protobuf/proto"
)

type CommandBuilder interface {
	Build() (Command, error)
}

type Command interface {
	Name() string
	Success() bool
	getRequestCode() byte
	constructPbRequest() (proto.Message, error)
	onError(error)
	onSuccess(proto.Message) error
	getExpectedResponseCode() byte
	getResponseProtobufMessage() proto.Message
}

func getRiakMessage(cmd Command) (msg []byte, err error) {
	requestCode := cmd.getRequestCode()
	if requestCode == 0 {
		panic(fmt.Sprintf("Must have non-zero value for getRequestCode(): %s", cmd.Name()))
	}

	var rpb proto.Message
	rpb, err = cmd.constructPbRequest()
	if err != nil {
		return
	}

	var bytes []byte
	if rpb != nil {
		bytes, err = proto.Marshal(rpb)
		if err != nil {
			return nil, err
		}
	}

	msg = buildRiakMessage(requestCode, bytes)
	return
}

func decodeRiakMessage(cmd Command, data []byte) (msg proto.Message, err error) {
	responseCode := cmd.getExpectedResponseCode()
	if responseCode == 0 {
		panic(fmt.Sprintf("Must have non-zero value for getExpectedResponseCode(): %s", cmd.Name()))
	}

	err = rpbValidateResp(data, responseCode)
	if err != nil {
		return
	}

	msg = cmd.getResponseProtobufMessage()
	if msg != nil {
		err = proto.Unmarshal(data[1:], msg)
	}

	return
}

func buildRiakMessage(code byte, data []byte) []byte {
	buf := new(bytes.Buffer)
	// write total message length, including one byte for msg code
	binary.Write(buf, binary.BigEndian, uint32(len(data)+1))
	// write the message code
	binary.Write(buf, binary.BigEndian, byte(code))
	// write the protobuf data
	buf.Write(data)
	return buf.Bytes()
}
