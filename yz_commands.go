package riak

import (
	rpbRiakYZ "github.com/basho-labs/riak-go-client/rpb/riak_yokozuna"
	proto "github.com/golang/protobuf/proto"
	"time"
)

type SearchIndex struct {
	Name string
	Schema string
	NVal uint32
}

// StoreIndex
// RpbYokozunaIndexPutReq

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
	return 0
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
	builder := &StoreIndexCommandBuilder{ protobuf: protobuf }
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
