package riak

import (
	rpbRiakDT "github.com/basho-labs/riak-go-client/rpb/riak_dt"
	proto "github.com/golang/protobuf/proto"
	"time"
)

// UpdateCounter
// DtUpdateReq
// DtUpdateResp

type UpdateCounterCommand struct {
	CommandImpl
	Response bool // TODO
	protobuf *rpbRiakDT.DtUpdateReq
}

func (cmd *UpdateCounterCommand) Name() string {
	return "UpdateCounter"
}

func (cmd *UpdateCounterCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *UpdateCounterCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	cmd.Response = true // TODO
	return nil
}

func (cmd *UpdateCounterCommand) getRequestCode() byte {
	return rpbCode_DtUpdateReq
}

func (cmd *UpdateCounterCommand) getResponseCode() byte {
	return rpbCode_DtUpdateResp
}

func (cmd *UpdateCounterCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakDT.DtUpdateResp{}
}

type UpdateCounterCommandBuilder struct {
	protobuf *rpbRiakDT.DtUpdateReq
}

func NewUpdateCounterCommandBuilder() *UpdateCounterCommandBuilder {
	return &UpdateCounterCommandBuilder{
		protobuf: &rpbRiakDT.DtUpdateReq{
			Op: &rpbRiakDT.DtOp{
				CounterOp: &rpbRiakDT.CounterOp{},
			},
		},
	}
}

func (builder *UpdateCounterCommandBuilder) WithBucketType(bucketType string) *UpdateCounterCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithBucket(bucket string) *UpdateCounterCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithKey(key string) *UpdateCounterCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithIncrement(increment int64) *UpdateCounterCommandBuilder {
	builder.protobuf.Op.CounterOp.Increment = &increment
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithW(w uint32) *UpdateCounterCommandBuilder {
	builder.protobuf.W = &w
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithPw(pw uint32) *UpdateCounterCommandBuilder {
	builder.protobuf.Pw = &pw
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithDw(dw uint32) *UpdateCounterCommandBuilder {
	builder.protobuf.Dw = &dw
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithReturnBody(returnBody bool) *UpdateCounterCommandBuilder {
	builder.protobuf.ReturnBody = &returnBody
	return builder
}

func (builder *UpdateCounterCommandBuilder) WithTimeout(timeout time.Duration) *UpdateCounterCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *UpdateCounterCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &UpdateCounterCommand{protobuf: builder.protobuf}, nil
}
