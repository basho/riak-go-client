package riak

import (
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"time"
)

// FetchValueCommand and FetchValueCommandBuilder

type FetchValueCommandBuilder struct {
	Options *FetchValueCommandOptions
}

func (builder *FetchValueCommandBuilder) Build() (Command, error) {
	if builder.Options == nil {
		return nil, ErrNilOptions
	}
	return NewFetchValueCommand(builder.Options)
}

type FetchValueCommandOptions struct {
	BucketType          string
	Bucket              string
	Key                 string
	R                   uint32
	Pr                  uint32
	BasicQuorum         bool
	NotFoundOk          bool
	IfNotModified       []byte // TODO pb field is IfModified
	HeadOnly            bool
	ReturnDeletedVClock bool
	Timeout             time.Duration
	SloppyQuorum        bool
	NVal                uint32
}

type FetchValueCommand struct {
	CommandImpl
	options *FetchValueCommandOptions
}

func NewFetchValueCommand(options *FetchValueCommandOptions) (cmd *FetchValueCommand, err error) {
	if options == nil {
		// TODO default options?
		err = ErrNilOptions
		return
	}
	cmd = &FetchValueCommand{
		options: options,
	}
	return
}

func (cmd *FetchValueCommand) Name() string {
	return "FetchValue"
}

func (cmd *FetchValueCommand) constructPbRequest() (msg proto.Message, err error) {
	// TODO
	msg = &rpbRiakKV.RpbGetReq{
		Type:   []byte(cmd.options.BucketType),
		Bucket: []byte(cmd.options.Bucket),
		Key:    []byte(cmd.options.Key),
	}
	return
}

func (cmd *FetchValueCommand) onSuccess(msg proto.Message) error {
	// TODO
	return nil
}

func (cmd *FetchValueCommand) getRequestCode() byte {
	return rpbCode_RpbGetReq
}

func (cmd *FetchValueCommand) getExpectedResponseCode() byte {
	return rpbCode_RpbGetResp
}

func (cmd *FetchValueCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbGetResp{}
}
