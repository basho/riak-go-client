package riak

import (
	"fmt"
	rpbRiakDT "github.com/basho-labs/riak-go-client/rpb/riak_dt"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"time"
)

// UpdateCounter
// DtUpdateReq
// DtUpdateResp

type UpdateCounterCommand struct {
	CommandImpl
	Response *UpdateCounterResponse
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
	if msg != nil {
		if rpbDtUpdateResp, ok := msg.(*rpbRiakDT.DtUpdateResp); ok {
			cmd.Response = &UpdateCounterResponse{
				GeneratedKey: string(rpbDtUpdateResp.GetKey()),
				CounterValue: rpbDtUpdateResp.GetCounterValue(),
			}
		} else {
			return fmt.Errorf("[UpdateCounterCommand] could not convert %v to DtUpdateResp", reflect.TypeOf(msg))
		}
	}
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

type UpdateCounterResponse struct {
	GeneratedKey string
	CounterValue int64
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

// FetchCounter
// DtFetchReq
// DtFetchResp

type FetchCounterCommand struct {
	CommandImpl
	Response *FetchCounterResponse
	protobuf *rpbRiakDT.DtFetchReq
}

func (cmd *FetchCounterCommand) Name() string {
	return "FetchCounter"
}

func (cmd *FetchCounterCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchCounterCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg != nil {
		if rpbDtFetchResp, ok := msg.(*rpbRiakDT.DtFetchResp); ok {
			response := &FetchCounterResponse{}
			rpbValue := rpbDtFetchResp.GetValue()
			if rpbValue == nil {
				response.IsNotFound = true
			} else {
				response.CounterValue = rpbValue.GetCounterValue()
			}
			cmd.Response = response
		} else {
			return fmt.Errorf("[FetchCounterCommand] could not convert %v to DtFetchResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchCounterCommand) getRequestCode() byte {
	return rpbCode_DtFetchReq
}

func (cmd *FetchCounterCommand) getResponseCode() byte {
	return rpbCode_DtFetchResp
}

func (cmd *FetchCounterCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakDT.DtFetchResp{}
}

type FetchCounterResponse struct {
	IsNotFound   bool
	CounterValue int64
}

type FetchCounterCommandBuilder struct {
	protobuf *rpbRiakDT.DtFetchReq
}

func NewFetchCounterCommandBuilder() *FetchCounterCommandBuilder {
	return &FetchCounterCommandBuilder{protobuf: &rpbRiakDT.DtFetchReq{}}
}

func (builder *FetchCounterCommandBuilder) WithBucketType(bucketType string) *FetchCounterCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *FetchCounterCommandBuilder) WithBucket(bucket string) *FetchCounterCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *FetchCounterCommandBuilder) WithKey(key string) *FetchCounterCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *FetchCounterCommandBuilder) WithR(r uint32) *FetchCounterCommandBuilder {
	builder.protobuf.R = &r
	return builder
}

func (builder *FetchCounterCommandBuilder) WithPr(pr uint32) *FetchCounterCommandBuilder {
	builder.protobuf.Pr = &pr
	return builder
}

func (builder *FetchCounterCommandBuilder) WithNotFoundOk(notFoundOk bool) *FetchCounterCommandBuilder {
	builder.protobuf.NotfoundOk = &notFoundOk
	return builder
}

func (builder *FetchCounterCommandBuilder) WithBasicQuorum(basicQuorum bool) *FetchCounterCommandBuilder {
	builder.protobuf.BasicQuorum = &basicQuorum
	return builder
}

func (builder *FetchCounterCommandBuilder) WithTimeout(timeout time.Duration) *FetchCounterCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *FetchCounterCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &FetchCounterCommand{protobuf: builder.protobuf}, nil
}

// UpdateSet
// DtUpdateReq
// DtUpdateResp

type UpdateSetCommand struct {
	CommandImpl
	Response *UpdateSetResponse
	protobuf *rpbRiakDT.DtUpdateReq
}

func (cmd *UpdateSetCommand) Name() string {
	return "UpdateSet"
}

func (cmd *UpdateSetCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *UpdateSetCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg != nil {
		if rpbDtUpdateResp, ok := msg.(*rpbRiakDT.DtUpdateResp); ok {
			response := &UpdateSetResponse{
				GeneratedKey: string(rpbDtUpdateResp.GetKey()),
				SetValue:     rpbDtUpdateResp.GetSetValue(),
			}
			cmd.Response = response
		} else {
			return fmt.Errorf("[UpdateSetCommand] could not convert %v to DtUpdateResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *UpdateSetCommand) getRequestCode() byte {
	return rpbCode_DtUpdateReq
}

func (cmd *UpdateSetCommand) getResponseCode() byte {
	return rpbCode_DtUpdateResp
}

func (cmd *UpdateSetCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakDT.DtUpdateResp{}
}

type UpdateSetResponse struct {
	GeneratedKey string
	SetValue     [][]byte
}

type UpdateSetCommandBuilder struct {
	protobuf *rpbRiakDT.DtUpdateReq
}

func NewUpdateSetCommandBuilder() *UpdateSetCommandBuilder {
	return &UpdateSetCommandBuilder{
		protobuf: &rpbRiakDT.DtUpdateReq{
			Op: &rpbRiakDT.DtOp{
				SetOp: &rpbRiakDT.SetOp{},
			},
		},
	}
}

func (builder *UpdateSetCommandBuilder) WithBucketType(bucketType string) *UpdateSetCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *UpdateSetCommandBuilder) WithBucket(bucket string) *UpdateSetCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *UpdateSetCommandBuilder) WithKey(key string) *UpdateSetCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *UpdateSetCommandBuilder) WithContext(context []byte) *UpdateSetCommandBuilder {
	builder.protobuf.Context = context
	return builder
}

func (builder *UpdateSetCommandBuilder) WithAdditions(adds ...[]byte) *UpdateSetCommandBuilder {
	opAdds := builder.protobuf.Op.SetOp.Adds
	if opAdds == nil {
		opAdds = make([][]byte, len(adds))
		for i, add := range adds {
			opAdds[i] = add
		}
	} else {
		opAdds = append(opAdds, adds...)
	}
	builder.protobuf.Op.SetOp.Adds = opAdds
	return builder
}

func (builder *UpdateSetCommandBuilder) WithRemovals(removals ...[]byte) *UpdateSetCommandBuilder {
	opRemoves := builder.protobuf.Op.SetOp.Removes
	if opRemoves == nil {
		opRemoves = make([][]byte, len(removals))
		for i, rem := range removals {
			opRemoves[i] = rem
		}
	} else {
		opRemoves = append(opRemoves, removals...)
	}
	builder.protobuf.Op.SetOp.Removes = opRemoves
	return builder
}

func (builder *UpdateSetCommandBuilder) WithW(w uint32) *UpdateSetCommandBuilder {
	builder.protobuf.W = &w
	return builder
}

func (builder *UpdateSetCommandBuilder) WithPw(pw uint32) *UpdateSetCommandBuilder {
	builder.protobuf.Pw = &pw
	return builder
}

func (builder *UpdateSetCommandBuilder) WithDw(dw uint32) *UpdateSetCommandBuilder {
	builder.protobuf.Dw = &dw
	return builder
}

func (builder *UpdateSetCommandBuilder) WithReturnBody(returnBody bool) *UpdateSetCommandBuilder {
	builder.protobuf.ReturnBody = &returnBody
	return builder
}

func (builder *UpdateSetCommandBuilder) WithTimeout(timeout time.Duration) *UpdateSetCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *UpdateSetCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &UpdateSetCommand{protobuf: builder.protobuf}, nil
}

// FetchSet
// DtFetchReq
// DtFetchResp

type FetchSetCommand struct {
	CommandImpl
	Response *FetchSetResponse
	protobuf *rpbRiakDT.DtFetchReq
}

func (cmd *FetchSetCommand) Name() string {
	return "FetchSet"
}

func (cmd *FetchSetCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchSetCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg != nil {
		if rpbDtFetchResp, ok := msg.(*rpbRiakDT.DtFetchResp); ok {
			response := &FetchSetResponse{}
			rpbValue := rpbDtFetchResp.GetValue()
			if rpbValue == nil {
				response.IsNotFound = true
			} else {
				response.SetValue = rpbValue.GetSetValue()
			}
			cmd.Response = response
		} else {
			return fmt.Errorf("[FetchSetCommand] could not convert %v to DtFetchResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchSetCommand) getRequestCode() byte {
	return rpbCode_DtFetchReq
}

func (cmd *FetchSetCommand) getResponseCode() byte {
	return rpbCode_DtFetchResp
}

func (cmd *FetchSetCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakDT.DtFetchResp{}
}

type FetchSetResponse struct {
	IsNotFound bool
	SetValue   [][]byte
}

type FetchSetCommandBuilder struct {
	protobuf *rpbRiakDT.DtFetchReq
}

func NewFetchSetCommandBuilder() *FetchSetCommandBuilder {
	return &FetchSetCommandBuilder{protobuf: &rpbRiakDT.DtFetchReq{}}
}

func (builder *FetchSetCommandBuilder) WithBucketType(bucketType string) *FetchSetCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *FetchSetCommandBuilder) WithBucket(bucket string) *FetchSetCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *FetchSetCommandBuilder) WithKey(key string) *FetchSetCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *FetchSetCommandBuilder) WithR(r uint32) *FetchSetCommandBuilder {
	builder.protobuf.R = &r
	return builder
}

func (builder *FetchSetCommandBuilder) WithPr(pr uint32) *FetchSetCommandBuilder {
	builder.protobuf.Pr = &pr
	return builder
}

func (builder *FetchSetCommandBuilder) WithNotFoundOk(notFoundOk bool) *FetchSetCommandBuilder {
	builder.protobuf.NotfoundOk = &notFoundOk
	return builder
}

func (builder *FetchSetCommandBuilder) WithBasicQuorum(basicQuorum bool) *FetchSetCommandBuilder {
	builder.protobuf.BasicQuorum = &basicQuorum
	return builder
}

func (builder *FetchSetCommandBuilder) WithTimeout(timeout time.Duration) *FetchSetCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *FetchSetCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &FetchSetCommand{protobuf: builder.protobuf}, nil
}
