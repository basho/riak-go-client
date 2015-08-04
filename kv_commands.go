package riak

import (
	"errors"
	"fmt"
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"strconv"
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

// Command used to delete a value from Riak.
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

// This builder type is required for creating new instances of the DeleteValue command.
//
//    deleteValue := NewDeleteValueCommandBuilder().
//        WithBucketType("myBucketType").
//        WithBucket("myBucket").
//        WithKey("myKey").
//        WithVClock(vclock).
//        Build()
type DeleteValueCommandBuilder struct {
	protobuf *rpbRiakKV.RpbDelReq
}

func NewDeleteValueCommandBuilder() *DeleteValueCommandBuilder {
	builder := &DeleteValueCommandBuilder{protobuf: &rpbRiakKV.RpbDelReq{}}
	return builder
}

// Set the bucket type.
//
// If not supplied, "default" is used.
func (builder *DeleteValueCommandBuilder) WithBucketType(bucketType string) *DeleteValueCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

// Set the bucket.
func (builder *DeleteValueCommandBuilder) WithBucket(bucket string) *DeleteValueCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

// Set the key.
func (builder *DeleteValueCommandBuilder) WithKey(key string) *DeleteValueCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

// Set the vector clock.
//
// If not set siblings may be created depending on bucket properties.
func (builder *DeleteValueCommandBuilder) WithVClock(vclock []byte) *DeleteValueCommandBuilder {
	builder.protobuf.Vclock = vclock
	return builder
}

// Set the R value.
//
// If not set the bucket default is used.
func (builder *DeleteValueCommandBuilder) WithR(r uint32) *DeleteValueCommandBuilder {
	builder.protobuf.R = &r
	return builder
}

// Set the W value.
//
// This represents the number of replicas to which to write before returning a successful response. If not set the bucket default is used.
func (builder *DeleteValueCommandBuilder) WithW(w uint32) *DeleteValueCommandBuilder {
	builder.protobuf.W = &w
	return builder
}

// Set the Pr value.
//
// If not set the bucket default is used.
func (builder *DeleteValueCommandBuilder) WithPr(pr uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Pr = &pr
	return builder
}

// Set the Pw value.
//
// This represents the number of primary nodes that must be available when the write is attempted. If not set the bucket default is used.
func (builder *DeleteValueCommandBuilder) WithPw(pw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Pw = &pw
	return builder
}

// Set the DW value.
//
// This represents the number of replicas to which to commit to durable storage before returning a successful response. If not set the bucket default is used.
func (builder *DeleteValueCommandBuilder) WithDw(dw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Dw = &dw
	return builder
}

// Set the RW value.
//
// This represents the quorum for both get and put operations involved in deleting an object .
func (builder *DeleteValueCommandBuilder) WithRw(rw uint32) *DeleteValueCommandBuilder {
	builder.protobuf.Rw = &rw
	return builder
}

// Set a timeout for this operation.
func (builder *DeleteValueCommandBuilder) WithTimeout(timeout time.Duration) *DeleteValueCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
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

// Command used to list buckets in a bucket type.
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
	if cmd.protobuf.GetStream() {
		return cmd.done
	} else {
		return true
	}
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
			cmd.done = rpbListBucketsResp.GetDone()
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
					response.Buckets = append(response.Buckets, buckets...)
				}
			}
		} else {
			cmd.done = true
			return fmt.Errorf("[ListBucketsCommand] could not convert %v to RpbListBucketsResp", reflect.TypeOf(msg))
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

// This builder type is required for creating new instances of the ListBucketsCommand.
//
//    cb := func(buckets []string) error {
//        // Do something with buckets
//        return nil
//    }
//    cmd := NewListBucketsCommandBuilder().
//        WithBucketType("myBucketType").
//        WithStreaming(true).
//        WithCallback(cb).
//        Build()
type ListBucketsCommandBuilder struct {
	callback func(buckets []string) error
	protobuf *rpbRiakKV.RpbListBucketsReq
}

func NewListBucketsCommandBuilder() *ListBucketsCommandBuilder {
	builder := &ListBucketsCommandBuilder{protobuf: &rpbRiakKV.RpbListBucketsReq{}}
	return builder
}

// Set the bucket type.
//
// If not supplied, "default" is used.
func (builder *ListBucketsCommandBuilder) WithBucketType(bucketType string) *ListBucketsCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

// Set to stream responses.
//
// If true, a callback must be provided via WithCallback()
func (builder *ListBucketsCommandBuilder) WithStreaming(streaming bool) *ListBucketsCommandBuilder {
	builder.protobuf.Stream = &streaming
	return builder
}

// Callback to use when streaming responses.
func (builder *ListBucketsCommandBuilder) WithCallback(callback func([]string) error) *ListBucketsCommandBuilder {
	builder.callback = callback
	return builder
}

// Set a timeout for this operation.
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

// ListKeys
// RpbListKeysReq
// RpbListKeysResp

type ListKeysCommand struct {
	CommandImpl
	Response  *ListKeysResponse
	protobuf  *rpbRiakKV.RpbListKeysReq
	streaming bool
	callback  func(keys []string) error
	done      bool
}

func (cmd *ListKeysCommand) Name() string {
	return "ListKeys"
}

func (cmd *ListKeysCommand) Done() bool {
	// NB: RpbListKeysReq is *always* streaming so no need to take
	// cmd.streaming into account here, unlike RpbListBucketsReq
	return cmd.done
}

func (cmd *ListKeysCommand) constructPbRequest() (msg proto.Message, err error) {
	msg = cmd.protobuf
	return
}

func (cmd *ListKeysCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.done = true
		cmd.Response = &ListKeysResponse{}
	} else {
		if rpbListKeysResp, ok := msg.(*rpbRiakKV.RpbListKeysResp); ok {
			cmd.done = rpbListKeysResp.GetDone()
			response := cmd.Response
			if response == nil {
				response = &ListKeysResponse{}
				cmd.Response = response
			}
			if rpbListKeysResp.GetKeys() != nil {
				keys := make([]string, len(rpbListKeysResp.GetKeys()))
				for i, key := range rpbListKeysResp.GetKeys() {
					keys[i] = string(key)
				}
				if cmd.streaming {
					if cmd.callback == nil {
						panic("ListKeysCommand requires a callback when streaming.")
					} else {
						if err := cmd.callback(keys); err != nil {
							cmd.Response = nil
							return err
						}
					}
				} else {
					response.Keys = append(response.Keys, keys...)
				}
			}
		} else {
			cmd.done = true
			return fmt.Errorf("[ListKeysCommand] could not convert %v to RpbListKeysResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *ListKeysCommand) getRequestCode() byte {
	return rpbCode_RpbListKeysReq
}

func (cmd *ListKeysCommand) getResponseCode() byte {
	return rpbCode_RpbListKeysResp
}

func (cmd *ListKeysCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbListKeysResp{}
}

type ListKeysResponse struct {
	Keys []string
}

type ListKeysCommandBuilder struct {
	protobuf  *rpbRiakKV.RpbListKeysReq
	streaming bool
	callback  func(buckets []string) error
}

func NewListKeysCommandBuilder() *ListKeysCommandBuilder {
	builder := &ListKeysCommandBuilder{protobuf: &rpbRiakKV.RpbListKeysReq{}}
	return builder
}

func (builder *ListKeysCommandBuilder) WithBucketType(bucketType string) *ListKeysCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *ListKeysCommandBuilder) WithBucket(bucket string) *ListKeysCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *ListKeysCommandBuilder) WithStreaming(streaming bool) *ListKeysCommandBuilder {
	builder.streaming = streaming
	return builder
}

func (builder *ListKeysCommandBuilder) WithCallback(callback func([]string) error) *ListKeysCommandBuilder {
	builder.callback = callback
	return builder
}

func (builder *ListKeysCommandBuilder) WithTimeout(timeout time.Duration) *ListKeysCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *ListKeysCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	if builder.streaming && builder.callback == nil {
		return nil, errors.New("ListKeysCommand requires a callback when streaming.")
	}
	return &ListKeysCommand{
		protobuf:  builder.protobuf,
		streaming: builder.streaming,
		callback:  builder.callback,
	}, nil
}

// FetchPreflist
// RpbGetBucketKeyPreflistReq
// RpbGetBucketKeyPreflistResp

type FetchPreflistCommand struct {
	CommandImpl
	Response *FetchPreflistResponse
	protobuf *rpbRiakKV.RpbGetBucketKeyPreflistReq
}

func (cmd *FetchPreflistCommand) Name() string {
	return "FetchPreflist"
}

func (cmd *FetchPreflistCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchPreflistCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.Response = &FetchPreflistResponse{}
	} else {
		if rpbGetBucketKeyPreflistResp, ok := msg.(*rpbRiakKV.RpbGetBucketKeyPreflistResp); ok {
			response := &FetchPreflistResponse{}
			if rpbGetBucketKeyPreflistResp.GetPreflist() != nil {
				rpbPreflist := rpbGetBucketKeyPreflistResp.GetPreflist()
				response.Preflist = make([]*PreflistItem, len(rpbPreflist))
				for i, rpbItem := range rpbPreflist {
					response.Preflist[i] = &PreflistItem{
						Partition: rpbItem.GetPartition(),
						Node:      string(rpbItem.GetNode()),
						Primary:   rpbItem.GetPrimary(),
					}
				}
			}
			cmd.Response = response
		} else {
			return fmt.Errorf("[FetchPreflistCommand] could not convert %v to RpbGetBucketKeyPreflistResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchPreflistCommand) getRequestCode() byte {
	return rpbCode_RpbGetBucketKeyPreflistReq
}

func (cmd *FetchPreflistCommand) getResponseCode() byte {
	return rpbCode_RpbGetBucketKeyPreflistResp
}

func (cmd *FetchPreflistCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbGetBucketKeyPreflistResp{}
}

type PreflistItem struct {
	Partition int64
	Node      string
	Primary   bool
}

type FetchPreflistResponse struct {
	Preflist []*PreflistItem
}

type FetchPreflistCommandBuilder struct {
	protobuf *rpbRiakKV.RpbGetBucketKeyPreflistReq
}

func NewFetchPreflistCommandBuilder() *FetchPreflistCommandBuilder {
	builder := &FetchPreflistCommandBuilder{protobuf: &rpbRiakKV.RpbGetBucketKeyPreflistReq{}}
	return builder
}

func (builder *FetchPreflistCommandBuilder) WithBucketType(bucketType string) *FetchPreflistCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *FetchPreflistCommandBuilder) WithBucket(bucket string) *FetchPreflistCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *FetchPreflistCommandBuilder) WithKey(key string) *FetchPreflistCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *FetchPreflistCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &FetchPreflistCommand{protobuf: builder.protobuf}, nil
}

// SecondaryIndexQuery
// RpbGetBucketKeyPreflistReq
// RpbGetBucketKeyPreflistResp

type SecondaryIndexQueryCommand struct {
	CommandImpl
	Response *SecondaryIndexQueryResponse
	protobuf *rpbRiakKV.RpbIndexReq
	callback func([]*SecondaryIndexQueryResult) error
	done     bool
}

func (cmd *SecondaryIndexQueryCommand) Done() bool {
	if cmd.protobuf.GetStream() {
		return cmd.done
	} else {
		return true
	}
}

func (cmd *SecondaryIndexQueryCommand) Name() string {
	return "SecondaryIndexQuery"
}

func (cmd *SecondaryIndexQueryCommand) constructPbRequest() (proto.Message, error) {
	if cmd.protobuf.GetKey() != nil {
		cmd.protobuf.Key = []byte(cmd.protobuf.GetKey())
		cmd.protobuf.Qtype = rpbRiakKV.RpbIndexReq_eq.Enum()
	} else {
		cmd.protobuf.Qtype = rpbRiakKV.RpbIndexReq_range.Enum()
	}
	return cmd.protobuf, nil
}

func (cmd *SecondaryIndexQueryCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.Response = &SecondaryIndexQueryResponse{}
		cmd.done = true
	} else {
		if rpbIndexResp, ok := msg.(*rpbRiakKV.RpbIndexResp); ok {
			cmd.done = rpbIndexResp.GetDone()
			response := cmd.Response
			if response == nil {
				response = &SecondaryIndexQueryResponse{}
				cmd.Response = response
			}

			response.Continuation = rpbIndexResp.GetContinuation()

			var results []*SecondaryIndexQueryResult
			rpbIndexRespResultsLen := len(rpbIndexResp.GetResults())
			if rpbIndexRespResultsLen > 0 {
				// Index keys and object keys were returned
				results = make([]*SecondaryIndexQueryResult, rpbIndexRespResultsLen)
				for i, rpbIndexResult := range rpbIndexResp.GetResults() {
					results[i] = &SecondaryIndexQueryResult{
						IndexKey:  rpbIndexResult.Key,
						ObjectKey: rpbIndexResult.Value,
					}
				}
			} else {
				// Only object keys were returned
				var key []byte
				if cmd.protobuf.GetReturnTerms() {
					// this is only possible if this was a single key query
					key = cmd.protobuf.GetKey()
				}
				rpbIndexRespKeys := rpbIndexResp.GetKeys()
				results = make([]*SecondaryIndexQueryResult, len(rpbIndexRespKeys))
				for i, rpbIndexKey := range rpbIndexRespKeys {
					results[i] = &SecondaryIndexQueryResult{
						IndexKey:  key,
						ObjectKey: rpbIndexKey,
					}
				}
			}

			if cmd.protobuf.GetStream() {
				if cmd.callback == nil {
					panic("SecondaryIndexQueryCommand requires a callback when streaming.")
				} else {
					if err := cmd.callback(results); err != nil {
						cmd.Response = nil
						return err
					}
				}
			} else {
				response.Results = append(response.Results, results...)
			}
		} else {
			cmd.done = true
			return fmt.Errorf("[SecondaryIndexQueryCommand] could not convert %v to RpbIndexResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *SecondaryIndexQueryCommand) getRequestCode() byte {
	return rpbCode_RpbIndexReq
}

func (cmd *SecondaryIndexQueryCommand) getResponseCode() byte {
	return rpbCode_RpbIndexResp
}

func (cmd *SecondaryIndexQueryCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbIndexResp{}
}

type SecondaryIndexQueryResult struct {
	IndexKey  []byte
	ObjectKey []byte
}

type SecondaryIndexQueryResponse struct {
	Results      []*SecondaryIndexQueryResult
	Continuation []byte
}

type SecondaryIndexQueryCommandBuilder struct {
	protobuf *rpbRiakKV.RpbIndexReq
	callback func([]*SecondaryIndexQueryResult) error
}

func NewSecondaryIndexQueryCommandBuilder() *SecondaryIndexQueryCommandBuilder {
	builder := &SecondaryIndexQueryCommandBuilder{protobuf: &rpbRiakKV.RpbIndexReq{}}
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithBucketType(bucketType string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithBucket(bucket string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithIndexName(indexName string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Index = []byte(indexName)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithRange(min string, max string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.RangeMin = []byte(min)
	builder.protobuf.RangeMax = []byte(max)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithIntRange(min int64, max int64) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.RangeMin = []byte(strconv.FormatInt(min, 10))
	builder.protobuf.RangeMax = []byte(strconv.FormatInt(max, 10))
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithIndexKey(key string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Key = []byte(key)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithReturnKeyAndIndex(val bool) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.ReturnTerms = &val
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithStreaming(streaming bool) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Stream = &streaming
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithCallback(callback func([]*SecondaryIndexQueryResult) error) *SecondaryIndexQueryCommandBuilder {
	builder.callback = callback
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithPaginationSort(paginationSort bool) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.PaginationSort = &paginationSort
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithMaxResults(maxResults uint32) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.MaxResults = &maxResults
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithContinuation(cont []byte) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.Continuation = cont
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithTermRegex(regex string) *SecondaryIndexQueryCommandBuilder {
	builder.protobuf.TermRegex = []byte(regex)
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) WithTimeout(timeout time.Duration) *SecondaryIndexQueryCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *SecondaryIndexQueryCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	if builder.protobuf.GetKey() == nil &&
		(builder.protobuf.GetRangeMin() == nil || builder.protobuf.GetRangeMax() == nil) {
		return nil, errors.New("either WithIndexKey or WithRange are required")
	}
	if builder.protobuf.GetStream() && builder.callback == nil {
		return nil, errors.New("SecondaryIndexQueryCommand requires a callback when streaming.")
	}
	return &SecondaryIndexQueryCommand{
		protobuf: builder.protobuf,
		callback: builder.callback,
	}, nil
}

// MapReduce
// RpbMapRedReq
// RpbMapRedResp

type MapReduceCommand struct {
	CommandImpl
	Response  [][]byte
	protobuf  *rpbRiakKV.RpbMapRedReq
	streaming bool
	callback  func(response []byte) error
	done      bool
}

func (cmd *MapReduceCommand) Name() string {
	return "MapReduce"
}

func (cmd *MapReduceCommand) Done() bool {
	// NB: RpbMapRedReq is *always* streaming so no need to take
	// cmd.streaming into account here, unlike RpbListBucketsReq
	return cmd.done
}

func (cmd *MapReduceCommand) constructPbRequest() (msg proto.Message, err error) {
	msg = cmd.protobuf
	return
}

func (cmd *MapReduceCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.done = true
	} else {
		if rpbMapRedResp, ok := msg.(*rpbRiakKV.RpbMapRedResp); ok {
			cmd.done = rpbMapRedResp.GetDone()
			rpbMapRedRespData := rpbMapRedResp.GetResponse()
			if cmd.streaming {
				if cmd.callback == nil {
					panic("MapReduceCommand requires a callback when streaming.")
				} else {
					if err := cmd.callback(rpbMapRedRespData); err != nil {
						cmd.Response = nil
						return err
					}
				}
			} else {
				cmd.Response = append(cmd.Response, rpbMapRedRespData)
			}
		} else {
			cmd.done = true
			return fmt.Errorf("[MapReduceCommand] could not convert %v to RpbMapRedResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *MapReduceCommand) getRequestCode() byte {
	return rpbCode_RpbMapRedReq
}

func (cmd *MapReduceCommand) getResponseCode() byte {
	return rpbCode_RpbMapRedResp
}

func (cmd *MapReduceCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakKV.RpbMapRedResp{}
}

type MapReduceCommandBuilder struct {
	protobuf  *rpbRiakKV.RpbMapRedReq
	streaming bool
	callback  func(response []byte) error
}

func NewMapReduceCommandBuilder() *MapReduceCommandBuilder {
	return &MapReduceCommandBuilder{
		protobuf: &rpbRiakKV.RpbMapRedReq{
			ContentType: []byte("application/json"),
		},
	}
}

func (builder *MapReduceCommandBuilder) WithQuery(query string) *MapReduceCommandBuilder {
	builder.protobuf.Request = []byte(query)
	return builder
}

func (builder *MapReduceCommandBuilder) WithStreaming(streaming bool) *MapReduceCommandBuilder {
	builder.streaming = streaming
	return builder
}

func (builder *MapReduceCommandBuilder) WithCallback(callback func([]byte) error) *MapReduceCommandBuilder {
	builder.callback = callback
	return builder
}

func (builder *MapReduceCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if builder.streaming && builder.callback == nil {
		return nil, errors.New("MapReduceCommand requires a callback when streaming.")
	}
	return &MapReduceCommand{
		protobuf:  builder.protobuf,
		streaming: builder.streaming,
		callback:  builder.callback,
	}, nil
}
