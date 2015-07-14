package riak

import (
	"fmt"
	rpbRiakYZ "github.com/basho-labs/riak-go-client/rpb/riak_yokozuna"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"time"
)

type SearchIndex struct {
	Name   string
	Schema string
	NVal   uint32
}

// StoreIndex
// RpbYokozunaIndexPutReq
// RpbPutResp

type StoreIndexCommand struct {
	CommandImpl
	Response bool
	protobuf *rpbRiakYZ.RpbYokozunaIndexPutReq
}

func (cmd *StoreIndexCommand) Name() string {
	return "StoreIndex"
}

func (cmd *StoreIndexCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *StoreIndexCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	cmd.Response = true
	return nil
}

func (cmd *StoreIndexCommand) getRequestCode() byte {
	return rpbCode_RpbYokozunaIndexPutReq
}

func (cmd *StoreIndexCommand) getResponseCode() byte {
	return rpbCode_RpbPutResp
}

func (cmd *StoreIndexCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

type StoreIndexCommandBuilder struct {
	protobuf *rpbRiakYZ.RpbYokozunaIndexPutReq
}

func NewStoreIndexCommandBuilder() *StoreIndexCommandBuilder {
	protobuf := &rpbRiakYZ.RpbYokozunaIndexPutReq{
		Index: &rpbRiakYZ.RpbYokozunaIndex{},
	}
	builder := &StoreIndexCommandBuilder{protobuf: protobuf}
	return builder
}

func (builder *StoreIndexCommandBuilder) WithIndexName(indexName string) *StoreIndexCommandBuilder {
	builder.protobuf.Index.Name = []byte(indexName)
	return builder
}

func (builder *StoreIndexCommandBuilder) WithSchemaName(schemaName string) *StoreIndexCommandBuilder {
	builder.protobuf.Index.Schema = []byte(schemaName)
	return builder
}

func (builder *StoreIndexCommandBuilder) WithNVal(nval uint32) *StoreIndexCommandBuilder {
	builder.protobuf.Index.NVal = &nval
	return builder
}

func (builder *StoreIndexCommandBuilder) WithTimeout(timeout time.Duration) *StoreIndexCommandBuilder {
	timeoutMilliseconds := uint32(timeout / time.Millisecond)
	builder.protobuf.Timeout = &timeoutMilliseconds
	return builder
}

func (builder *StoreIndexCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	return &StoreIndexCommand{protobuf: builder.protobuf}, nil
}

// FetchIndex
// RpbYokozunaIndexGetReq
// RpbYokozunaIndexGetResp

type FetchIndexCommand struct {
	CommandImpl
	Response []*SearchIndex
	protobuf *rpbRiakYZ.RpbYokozunaIndexGetReq
}

func (cmd *FetchIndexCommand) Name() string {
	return "FetchIndex"
}

func (cmd *FetchIndexCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchIndexCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg != nil {
		if rpbYokozunaIndexGetResp, ok := msg.(*rpbRiakYZ.RpbYokozunaIndexGetResp); ok {
			rpbIndexes := rpbYokozunaIndexGetResp.GetIndex()
			if rpbIndexes != nil {
				cmd.Response = make([]*SearchIndex, len(rpbIndexes))
				for i, rpbIndex := range rpbIndexes {
					index := &SearchIndex{
						Name:   string(rpbIndex.GetName()),
						Schema: string(rpbIndex.GetSchema()),
						NVal:   rpbIndex.GetNVal(),
					}
					cmd.Response[i] = index
				}
			}
		} else {
			return fmt.Errorf("[FetchIndexCommand] could not convert %v to RpbYokozunaIndexGetResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchIndexCommand) getRequestCode() byte {
	return rpbCode_RpbYokozunaIndexGetReq
}

func (cmd *FetchIndexCommand) getResponseCode() byte {
	return rpbCode_RpbYokozunaIndexGetResp
}

func (cmd *FetchIndexCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiakYZ.RpbYokozunaIndexGetResp{}
}

type FetchIndexCommandBuilder struct {
	protobuf *rpbRiakYZ.RpbYokozunaIndexGetReq
}

func NewFetchIndexCommandBuilder() *FetchIndexCommandBuilder {
	builder := &FetchIndexCommandBuilder{protobuf: &rpbRiakYZ.RpbYokozunaIndexGetReq{}}
	return builder
}

func (builder *FetchIndexCommandBuilder) WithIndexName(indexName string) *FetchIndexCommandBuilder {
	builder.protobuf.Name = []byte(indexName)
	return builder
}

func (builder *FetchIndexCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	return &FetchIndexCommand{protobuf: builder.protobuf}, nil
}

// DeleteIndex
// RpbYokozunaIndexDeleteReq
// RpbDelResp

type DeleteIndexCommand struct {
	CommandImpl
	Response bool
	protobuf *rpbRiakYZ.RpbYokozunaIndexDeleteReq
}

func (cmd *DeleteIndexCommand) Name() string {
	return "DeleteIndex"
}

func (cmd *DeleteIndexCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *DeleteIndexCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	cmd.Response = true
	return nil
}

func (cmd *DeleteIndexCommand) getRequestCode() byte {
	return rpbCode_RpbYokozunaIndexDeleteReq
}

func (cmd *DeleteIndexCommand) getResponseCode() byte {
	return rpbCode_RpbDelResp
}

func (cmd *DeleteIndexCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

type DeleteIndexCommandBuilder struct {
	protobuf *rpbRiakYZ.RpbYokozunaIndexDeleteReq
}

func NewDeleteIndexCommandBuilder() *DeleteIndexCommandBuilder {
	builder := &DeleteIndexCommandBuilder{protobuf: &rpbRiakYZ.RpbYokozunaIndexDeleteReq{}}
	return builder
}

func (builder *DeleteIndexCommandBuilder) WithIndexName(indexName string) *DeleteIndexCommandBuilder {
	builder.protobuf.Name = []byte(indexName)
	return builder
}

func (builder *DeleteIndexCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	return &DeleteIndexCommand{protobuf: builder.protobuf}, nil
}
