package riak

import (
	"fmt"
	rpbRiak "github.com/basho-labs/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
	"reflect"
)

// Ping

type PingCommandBuilder struct {
}

func (builder *PingCommandBuilder) Build() (Command, error) {
	return &PingCommand{}, nil
}

type PingCommand struct {
	CommandImpl
}

func (cmd *PingCommand) Name() string {
	return "Ping"
}

func (cmd *PingCommand) getRequestCode() byte {
	return rpbCode_RpbPingReq
}

func (cmd *PingCommand) constructPbRequest() (msg proto.Message, err error) {
	return nil, nil
}

func (cmd *PingCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	return nil
}

func (cmd *PingCommand) getExpectedResponseCode() byte {
	return rpbCode_RpbPingResp
}

func (cmd *PingCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

// FetchBucketProps

type FetchBucketPropsCommand struct {
	CommandImpl
	Response *FetchBucketPropsResponse
	protobuf *rpbRiak.RpbGetBucketReq
}

func (cmd *FetchBucketPropsCommand) Name() string {
	return "FetchBucketProps"
}

func (cmd *FetchBucketPropsCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *FetchBucketPropsCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	if msg == nil {
		cmd.Success = false
	} else {
		if rpbGetBucketResp, ok := msg.(*rpbRiak.RpbGetBucketResp); ok {
			rpbBucketProps := rpbGetBucketResp.GetProps()
			cmd.Response = &FetchBucketPropsResponse{
				NVal:          rpbBucketProps.GetNVal(),
				AllowMult:     rpbBucketProps.GetAllowMult(),
				LastWriteWins: rpbBucketProps.GetLastWriteWins(),
				HasPrecommit:  rpbBucketProps.GetHasPrecommit(),
				HasPostcommit: rpbBucketProps.GetHasPostcommit(),
				OldVClock:     rpbBucketProps.GetOldVclock(),
				YoungVClock:   rpbBucketProps.GetYoungVclock(),
				BigVClock:     rpbBucketProps.GetBigVclock(),
				SmallVClock:   rpbBucketProps.GetSmallVclock(),
				R:             rpbBucketProps.GetR(),
				Pr:            rpbBucketProps.GetPr(),
				W:             rpbBucketProps.GetW(),
				Pw:            rpbBucketProps.GetPw(),
				Dw:            rpbBucketProps.GetDw(),
				Rw:            rpbBucketProps.GetRw(),
				BasicQuorum:   rpbBucketProps.GetBasicQuorum(),
				NotFoundOk:    rpbBucketProps.GetNotfoundOk(),
				Search:        rpbBucketProps.GetSearch(),
				Consistent:    rpbBucketProps.GetConsistent(),
				Repl:          ReplMode(rpbBucketProps.GetRepl()),
				Backend:       string(rpbBucketProps.GetBackend()),
				SearchIndex:   string(rpbBucketProps.GetSearchIndex()),
				DataType:      string(rpbBucketProps.GetDatatype()),
			}
		} else {
			return fmt.Errorf("[FetchBucketPropsCommand] could not convert %v to RpbGetResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchBucketPropsCommand) getRequestCode() byte {
	return rpbCode_RpbGetReq
}

func (cmd *FetchBucketPropsCommand) getExpectedResponseCode() byte {
	return rpbCode_RpbGetResp
}

func (cmd *FetchBucketPropsCommand) getResponseProtobufMessage() proto.Message {
	return &rpbRiak.RpbGetBucketResp{}
}

type ReplMode int32

const (
	FALSE    ReplMode = 0
	REALTIME ReplMode = 1
	FULLSYNC ReplMode = 2
	TRUE     ReplMode = 3
)

type FetchBucketPropsResponse struct {
	NVal          uint32
	AllowMult     bool
	LastWriteWins bool
	HasPrecommit  bool
	HasPostcommit bool
	OldVClock     uint32
	YoungVClock   uint32
	BigVClock     uint32
	SmallVClock   uint32
	R             uint32
	Pr            uint32
	W             uint32
	Pw            uint32
	Dw            uint32
	Rw            uint32
	BasicQuorum   bool
	NotFoundOk    bool
	Search        bool
	Consistent    bool
	Repl          ReplMode
	Backend       string
	SearchIndex   string
	DataType      string
}

type FetchBucketPropsCommandBuilder struct {
	protobuf *rpbRiak.RpbGetBucketReq
}

func NewFetchBucketPropsCommandBuilder() *FetchBucketPropsCommandBuilder {
	builder := &FetchBucketPropsCommandBuilder{protobuf: &rpbRiak.RpbGetBucketReq{}}
	return builder
}

func (builder *FetchBucketPropsCommandBuilder) WithBucketType(bucketType string) *FetchBucketPropsCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *FetchBucketPropsCommandBuilder) WithBucket(bucket string) *FetchBucketPropsCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *FetchBucketPropsCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &FetchBucketPropsCommand{protobuf: builder.protobuf}, nil
}
