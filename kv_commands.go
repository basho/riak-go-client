package riak

import (
	"fmt"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"time"
)

// FetchValue

type ConflictResolver interface {
	Resolve([]*Object) []*Object
}

type FetchValueCommand struct {
	CommandImpl
	Response *FetchValueResponse
	protobuf *rpbRiakKV.RpbGetReq
	resolver ConflictResolver
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
						response.Values[i] = ro
					}
				}
				if cmd.resolver != nil {
					response.Values = cmd.resolver.Resolve(response.Values)
				}
			}

			cmd.Response = response
		} else {
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

type FetchValueResponse struct {
	IsNotFound  bool
	IsUnchanged bool
	VClock      []byte
	Values      []*Object
}

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

// StoreValue

type StoreValueCommand struct {
	CommandImpl
	Response *StoreValueResponse
	protobuf *rpbRiakKV.RpbPutReq
	resolver ConflictResolver
}

func (cmd *StoreValueCommand) Name() string {
	return "StoreValue"
}

func (cmd *StoreValueCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *StoreValueCommand) onSuccess(msg proto.Message) error {
	if msg == nil {
		// TODO error?
		cmd.Response = &StoreValueResponse{}
	} else {
		if rpbPutResp, ok := msg.(*rpbRiakKV.RpbPutResp); ok {
			var responseKey string
			if responseKeyBytes := rpbPutResp.GetKey(); responseKeyBytes != nil && len(responseKeyBytes) > 0 {
				responseKey = string(responseKeyBytes)
			}

			vclock := rpbPutResp.GetVclock()
			response := &StoreValueResponse{
				VClock:       vclock,
				GeneratedKey: responseKey,
			}

			if pbContent := rpbPutResp.GetContent(); pbContent != nil && len(pbContent) > 0 {
				response.Values = make([]*Object, len(pbContent))
				for i, content := range pbContent {
					if ro, err := NewObjectFromRpbContent(content); err != nil {
						return err
					} else {
						ro.VClock = vclock
						ro.BucketType = string(cmd.protobuf.Type)
						ro.Bucket = string(cmd.protobuf.Bucket)
						if responseKey == "" {
							ro.Key = string(cmd.protobuf.Key)
						} else {
							ro.Key = responseKey
						}
						response.Values[i] = ro
					}
				}
				if cmd.resolver != nil {
					response.Values = cmd.resolver.Resolve(response.Values)
				}
			}

			cmd.Response = response
		} else {
			return fmt.Errorf("[StoreValueCommand] could not convert %v to RpbPutResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *StoreValueCommand) getRequestCode() byte {
	return rpbCode_RpbGetReq
}

func (cmd *StoreValueCommand) getExpectedResponseCode() byte {
	return rpbCode_RpbGetResp
}

func (cmd *StoreValueCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbGetResp{}
}

type StoreValueResponse struct {
	GeneratedKey string
	VClock       []byte
	Values       []*Object
}

type StoreValueCommandBuilder struct {
	content  *Object
	protobuf *rpbRiakKV.RpbPutReq
	resolver ConflictResolver
}

func NewStoreValueCommandBuilder() *StoreValueCommandBuilder {
	builder := &StoreValueCommandBuilder{protobuf: &rpbRiakKV.RpbPutReq{}}
	return builder
}

func (builder *StoreValueCommandBuilder) WithConflictResolver(resolver ConflictResolver) *StoreValueCommandBuilder {
	builder.resolver = resolver
	return builder
}

func (builder *StoreValueCommandBuilder) WithBucketType(bucketType string) *StoreValueCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *StoreValueCommandBuilder) WithBucket(bucket string) *StoreValueCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *StoreValueCommandBuilder) WithKey(key string) *StoreValueCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *StoreValueCommandBuilder) WithVClock(vclock []byte) *StoreValueCommandBuilder {
	builder.protobuf.Vclock = vclock
	return builder
}

func (builder *StoreValueCommandBuilder) WithContent(object *Object) *StoreValueCommandBuilder {
	builder.content = object
	return builder
}

func (builder *StoreValueCommandBuilder) WithW(w uint32) *StoreValueCommandBuilder {
	builder.protobuf.W = &w
	return builder
}

func (builder *StoreValueCommandBuilder) WithDw(dw uint32) *StoreValueCommandBuilder {
	builder.protobuf.Dw = &dw
	return builder
}

func (builder *StoreValueCommandBuilder) WithPw(pw uint32) *StoreValueCommandBuilder {
	builder.protobuf.Pw = &pw
	return builder
}

func (builder *StoreValueCommandBuilder) WithReturnBody(returnBody bool) *StoreValueCommandBuilder {
	builder.protobuf.ReturnBody = &returnBody
	return builder
}

func (builder *StoreValueCommandBuilder) WithIfNotModified(ifNotModified bool) *StoreValueCommandBuilder {
	builder.protobuf.IfNotModified = &ifNotModified
	return builder
}

func (builder *StoreValueCommandBuilder) WithIfNoneMatch(ifNoneMatch bool) *StoreValueCommandBuilder {
	builder.protobuf.IfNoneMatch = &ifNoneMatch
	return builder
}

func (builder *StoreValueCommandBuilder) WithReturnHead(returnHead bool) *StoreValueCommandBuilder {
	builder.protobuf.ReturnHead = &returnHead
	return builder
}

func (builder *StoreValueCommandBuilder) WithTimeout(timeout time.Duration) *StoreValueCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *StoreValueCommandBuilder) WithAsis(asis bool) *StoreValueCommandBuilder {
	builder.protobuf.Asis = &asis
	return builder
}

func (builder *StoreValueCommandBuilder) WithSloppyQuorum(sloppyQuorum bool) *StoreValueCommandBuilder {
	builder.protobuf.SloppyQuorum = &sloppyQuorum
	return builder
}

func (builder *StoreValueCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &StoreValueCommand{protobuf: builder.protobuf}, nil
}
