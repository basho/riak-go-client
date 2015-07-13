package riak

import (
	"errors"
	"fmt"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"time"
)

// FetchValue
// RpbGetReq
// RpbGetResp

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
	cmd.Success = true
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
					if ro, err := fromRpbContent(content); err != nil {
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

func (cmd *FetchValueCommand) getResponseCode() byte {
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
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &FetchValueCommand{protobuf: builder.protobuf}, nil
}

// StoreValue
// RpbPutReq
// RpbPutResp

type StoreValueCommand struct {
	CommandImpl
	Response *StoreValueResponse
	value    *Object
	protobuf *rpbRiakKV.RpbPutReq
	resolver ConflictResolver
}

func (cmd *StoreValueCommand) Name() string {
	return "StoreValue"
}

func (cmd *StoreValueCommand) constructPbRequest() (msg proto.Message, err error) {
	value := cmd.value

	// Some properties of the value override options
	if value.VClock != nil {
		cmd.protobuf.Vclock = value.VClock
	}
	if value.BucketType != "" {
		cmd.protobuf.Type = []byte(value.BucketType)
	}
	if value.Bucket != "" {
		cmd.protobuf.Bucket = []byte(value.Bucket)
	}
	if value.Key != "" {
		cmd.protobuf.Key = []byte(value.Key)
	}

	cmd.protobuf.Content, err = toRpbContent(value)
	if err != nil {
		return
	}

	msg = cmd.protobuf
	return
}

func (cmd *StoreValueCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
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
					if ro, err := fromRpbContent(content); err != nil {
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
	return rpbCode_RpbPutReq
}

func (cmd *StoreValueCommand) getResponseCode() byte {
	return rpbCode_RpbPutResp
}

func (cmd *StoreValueCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbPutResp{}
}

type StoreValueResponse struct {
	GeneratedKey string
	VClock       []byte
	Values       []*Object
}

type StoreValueCommandBuilder struct {
	value    *Object
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
	builder.value = object
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

func (builder *StoreValueCommandBuilder) WithNVal(nval uint32) *StoreValueCommandBuilder {
	builder.protobuf.NVal = &nval
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
	return &StoreValueCommand{value: builder.value, protobuf: builder.protobuf}, nil
}

// DeleteValue
// RpbDelReq
// RpbDelResp

type DeleteValueCommand struct {
	CommandImpl
	Response bool
	protobuf *rpbRiakKV.RpbDelReq
}

func (cmd *DeleteValueCommand) Name() string {
	return "DeleteValue"
}

func (cmd *DeleteValueCommand) constructPbRequest() (msg proto.Message, err error) {
	msg = cmd.protobuf
	return
}

func (cmd *DeleteValueCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	cmd.Response = true
	return nil
}

func (cmd *DeleteValueCommand) getRequestCode() byte {
	return rpbCode_RpbDelReq
}

func (cmd *DeleteValueCommand) getResponseCode() byte {
	return rpbCode_RpbDelResp
}

func (cmd *DeleteValueCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

type DeleteValueCommandBuilder struct {
	protobuf *rpbRiakKV.RpbDelReq
}

func NewDeleteValueCommandBuilder() *DeleteValueCommandBuilder {
	builder := &DeleteValueCommandBuilder{protobuf: &rpbRiakKV.RpbDelReq{}}
	return builder
}

func (builder *DeleteValueCommandBuilder) WithBucketType(bucketType string) *DeleteValueCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *DeleteValueCommandBuilder) WithBucket(bucket string) *DeleteValueCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *DeleteValueCommandBuilder) WithKey(key string) *DeleteValueCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *DeleteValueCommandBuilder) WithVClock(vclock []byte) *DeleteValueCommandBuilder {
	builder.protobuf.Vclock = vclock
	return builder
}

func (builder *DeleteValueCommandBuilder) WithR(r uint32) *DeleteValueCommandBuilder {
	builder.protobuf.R = &r
	return builder
}

func (builder *DeleteValueCommandBuilder) WithW(w uint32) *DeleteValueCommandBuilder {
	builder.protobuf.W = &w
	return builder
}

func (builder *DeleteValueCommandBuilder) WithPr(pr uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Pr = &pr
	return builder
}

func (builder *DeleteValueCommandBuilder) WithPw(pw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Pw = &pw
	return builder
}

func (builder *DeleteValueCommandBuilder) WithDw(dw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Dw = &dw
	return builder
}

func (builder *DeleteValueCommandBuilder) WithRw(rw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Rw = &rw
	return builder
}

func (builder *DeleteValueCommandBuilder) WithTimeout(timeout time.Duration) *DeleteValueCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *DeleteValueCommandBuilder) WithSloppyQuorum(sloppyQuorum bool) *DeleteValueCommandBuilder {
	builder.protobuf.SloppyQuorum = &sloppyQuorum
	return builder
}

func (builder *DeleteValueCommandBuilder) WithNVal(nval uint32) *DeleteValueCommandBuilder {
	builder.protobuf.NVal = &nval
	return builder
}

func (builder *DeleteValueCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &DeleteValueCommand{protobuf: builder.protobuf}, nil
}

// ListBuckets
// RpbListBucketsReq
// RpbListBucketsResp

type ListBucketsCommand struct {
	CommandImpl
	Response *ListBucketsResponse
	protobuf *rpbRiakKV.RpbListBucketsReq
	callback func(buckets []string) error
	done     bool
}

func (cmd *ListBucketsCommand) Name() string {
	return "ListBuckets"
}

func (cmd *ListBucketsCommand) Done() bool {
	return cmd.done
}

func (cmd *ListBucketsCommand) constructPbRequest() (msg proto.Message, err error) {
	msg = cmd.protobuf
	return
}

func (cmd *ListBucketsCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.done = true
		cmd.Response = &ListBucketsResponse{}
	} else {
		if rpbListBucketsResp, ok := msg.(*rpbRiakKV.RpbListBucketsResp); ok {
			if rpbListBucketsResp.Done == nil {
				cmd.done = true
			} else {
				cmd.done = rpbListBucketsResp.GetDone()
			}

			response := cmd.Response
			if response == nil {
				response = &ListBucketsResponse{}
				cmd.Response = response
			}

			if rpbListBucketsResp.GetBuckets() != nil {
				buckets := make([]string, len(rpbListBucketsResp.GetBuckets()))
				for i, bucket := range rpbListBucketsResp.GetBuckets() {
					buckets[i] = string(bucket)
				}

				if cmd.protobuf.GetStream() {
					if cmd.callback == nil {
						panic("ListBucketsCommand requires a callback when streaming.")
					} else {
						if err := cmd.callback(buckets); err != nil {
							cmd.Response = nil
							return err
						}
					}
				} else {
					if response.Buckets == nil {
						response.Buckets = buckets
					} else {
						response.Buckets = append(response.Buckets, buckets...)
					}
				}
			}
		} else {
			return fmt.Errorf("[StoreValueCommand] could not convert %v to RpbPutResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *ListBucketsCommand) getRequestCode() byte {
	return rpbCode_RpbListBucketsReq
}

func (cmd *ListBucketsCommand) getResponseCode() byte {
	return rpbCode_RpbListBucketsResp
}

func (cmd *ListBucketsCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbListBucketsResp{}
}

type ListBucketsResponse struct {
	Buckets []string
}

type ListBucketsCommandBuilder struct {
	callback func(buckets []string) error
	protobuf *rpbRiakKV.RpbListBucketsReq
}

func NewListBucketsCommandBuilder() *ListBucketsCommandBuilder {
	builder := &ListBucketsCommandBuilder{protobuf: &rpbRiakKV.RpbListBucketsReq{}}
	return builder
}

func (builder *ListBucketsCommandBuilder) WithBucketType(bucketType string) *ListBucketsCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *ListBucketsCommandBuilder) WithStreaming(streaming bool) *ListBucketsCommandBuilder {
	builder.protobuf.Stream = &streaming
	return builder
}

func (builder *ListBucketsCommandBuilder) WithCallback(callback func([]string) error) *ListBucketsCommandBuilder {
	builder.callback = callback
	return builder
}

func (builder *ListBucketsCommandBuilder) WithTimeout(timeout time.Duration) *ListBucketsCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *ListBucketsCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	if builder.protobuf.GetStream() && builder.callback == nil {
		return nil, errors.New("ListBucketsCommand requires a callback when streaming.")
	}
	return &ListBucketsCommand{protobuf: builder.protobuf, callback: builder.callback}, nil
}
