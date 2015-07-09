package riak

import (
	"fmt"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"time"
)

type ConflictResolver interface {
	Resolve([]*Object) *Object
}

// FetchValueCommand

type FetchValueCommand struct {
	CommandImpl
	protobuf *rpbRiakKV.RpbGetReq
	Response *FetchValueResponse
}

func (cmd *FetchValueCommand) Name() string {
	return "FetchValue"
}

func (cmd *FetchValueCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchValueCommand) onSuccess(msg proto.Message) error {
	if msg == nil {
		cmd.Response = &FetchValueResponse{
			IsNotFound:  true,
			IsUnchanged: false,
		}
	} else {
		if rpbGetResp, ok := msg.(*rpbRiakKV.RpbGetResp); ok {
			vclock := rpbGetResp.GetVclock()
			response := &FetchValueResponse{
				VClock:      vclock,
				IsUnchanged: rpbGetResp.GetUnchanged(),
				IsNotFound:  false,
			}

			if pbContent := rpbGetResp.GetContent(); pbContent == nil || len(pbContent) == 0 {
				object := &Object{
					IsTombstone: true,
					BucketType:  string(cmd.protobuf.Type),
					Bucket:      string(cmd.protobuf.Bucket),
					Key:         string(cmd.protobuf.Key),
				}
				response.Values = []*Object{object}
			} else {
				response.Values = make([]*Object, len(pbContent))
				for i, content := range pbContent {
					if ro, err := NewObjectFromRpbContent(content); err != nil {
						return err
					} else {
						ro.VClock = vclock
						ro.BucketType = string(cmd.protobuf.Type)
						ro.Bucket = string(cmd.protobuf.Bucket)
						ro.Key = string(cmd.protobuf.Key)
						/*
							* TODO
							if (this.options.conflictResolver) {
								values = [this.options.conflictResolver(values)];
							}
						*/
						response.Values[i] = ro
					}
				}
			}

			cmd.Response = response
		} else {
			// TODO specific Riak error?
			return fmt.Errorf("[FetchValueCommand] could not convert %v to RpbGetResp", reflect.TypeOf(msg))
		}
	}
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

// FetchValueResponse

type FetchValueResponse struct {
	IsNotFound  bool
	IsUnchanged bool
	VClock      []byte
	Values      []*Object
}

// FetchValueCommandBuilder

type FetchValueCommandBuilder struct {
	protobuf *rpbRiakKV.RpbGetReq
	resolver ConflictResolver
}

func NewFetchValueCommandBuilder() *FetchValueCommandBuilder {
	builder := &FetchValueCommandBuilder{protobuf: &rpbRiakKV.RpbGetReq{}}
	return builder
}

func (builder *FetchValueCommandBuilder) WithConflictResolver(resolver ConflictResolver) *FetchValueCommandBuilder {
	builder.resolver = resolver
	return builder
}

func (builder *FetchValueCommandBuilder) WithBucketType(bucketType string) *FetchValueCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *FetchValueCommandBuilder) WithBucket(bucket string) *FetchValueCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *FetchValueCommandBuilder) WithKey(key string) *FetchValueCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *FetchValueCommandBuilder) WithR(r uint32) *FetchValueCommandBuilder {
	builder.protobuf.R = &r
	return builder
}

func (builder *FetchValueCommandBuilder) WithPr(pr uint32) *FetchValueCommandBuilder {
	builder.protobuf.Pr = &pr
	return builder
}

func (builder *FetchValueCommandBuilder) WithNVal(nval uint32) *FetchValueCommandBuilder {
	builder.protobuf.NVal = &nval
	return builder
}

func (builder *FetchValueCommandBuilder) WithBasicQuorum(basicQuorum bool) *FetchValueCommandBuilder {
	builder.protobuf.BasicQuorum = &basicQuorum
	return builder
}

func (builder *FetchValueCommandBuilder) WithNotFoundOk(notFoundOk bool) *FetchValueCommandBuilder {
	builder.protobuf.NotfoundOk = &notFoundOk
	return builder
}

func (builder *FetchValueCommandBuilder) WithIfNotModified(ifNotModified []byte) *FetchValueCommandBuilder {
	builder.protobuf.IfModified = ifNotModified
	return builder
}

func (builder *FetchValueCommandBuilder) WithHeadOnly(headOnly bool) *FetchValueCommandBuilder {
	builder.protobuf.Head = &headOnly
	return builder
}

func (builder *FetchValueCommandBuilder) WithReturnDeletedVClock(returnDeletedVClock bool) *FetchValueCommandBuilder {
	builder.protobuf.Deletedvclock = &returnDeletedVClock
	return builder
}

func (builder *FetchValueCommandBuilder) WithTimeout(timeout time.Duration) *FetchValueCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *FetchValueCommandBuilder) WithSloppyQuorum(sloppyQuorum bool) *FetchValueCommandBuilder {
	builder.protobuf.SloppyQuorum = &sloppyQuorum
	return builder
}

func (builder *FetchValueCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	// TODO refactor this out somehow for other commands that use BT/B/K
	if builder.protobuf.Type == nil {
		builder.protobuf.Type = []byte(defaultBucketType)
	}
	if builder.protobuf.Bucket == nil {
		return nil, ErrBucketRequired
	}
	if builder.protobuf.Key == nil {
		return nil, ErrKeyRequired
	}
	return &FetchValueCommand{protobuf: builder.protobuf}, nil
}
