package riak

import (
	"fmt"
	rpbRiak "github.com/basho/riak-go-client/rpb/riak"
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

func (cmd *PingCommand) getResponseCode() byte {
	return rpbCode_RpbPingResp
}

func (cmd *PingCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

// StartTls

type StartTlsCommand struct {
	CommandImpl
}

func (cmd *StartTlsCommand) Name() string {
	return "StartTls"
}

func (cmd *StartTlsCommand) constructPbRequest() (msg proto.Message, err error) {
	return nil, nil
}

func (cmd *StartTlsCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	return nil
}

func (cmd *StartTlsCommand) getRequestCode() byte {
	return rpbCode_RpbStartTls
}

func (cmd *StartTlsCommand) getResponseCode() byte {
	return rpbCode_RpbStartTls
}

func (cmd *StartTlsCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

// Auth

type AuthCommand struct {
	CommandImpl
	User     string
	Password string
}

func (cmd *AuthCommand) Name() string {
	return "Auth"
}

func (cmd *AuthCommand) constructPbRequest() (msg proto.Message, err error) {
	return &rpbRiak.RpbAuthReq{
		User:     []byte(cmd.User),
		Password: []byte(cmd.Password),
	}, nil
}

func (cmd *AuthCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	return nil
}

func (cmd *AuthCommand) getRequestCode() byte {
	return rpbCode_RpbAuthReq
}

func (cmd *AuthCommand) getResponseCode() byte {
	return rpbCode_RpbAuthResp
}

func (cmd *AuthCommand) getResponseProtobufMessage() proto.Message {
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
			response := &FetchBucketPropsResponse{
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

			if rpbBucketProps.GetHasPrecommit() {
				response.PreCommit = getHooksFrom(rpbBucketProps.Precommit)
			}
			if rpbBucketProps.GetHasPostcommit() {
				response.PostCommit = getHooksFrom(rpbBucketProps.Postcommit)
			}
			if rpbBucketProps.ChashKeyfun != nil {
				response.ChashKeyFun = getFunFrom(rpbBucketProps.ChashKeyfun)
			}
			if rpbBucketProps.Linkfun != nil {
				response.LinkFun = getFunFrom(rpbBucketProps.Linkfun)
			}

			cmd.Response = response
		} else {
			return fmt.Errorf("[FetchBucketPropsCommand] could not convert %v to RpbGetResp", reflect.TypeOf(msg))
		}
	}
	return nil
}

func (cmd *FetchBucketPropsCommand) getRequestCode() byte {
	return rpbCode_RpbGetBucketReq
}

func (cmd *FetchBucketPropsCommand) getResponseCode() byte {
	return rpbCode_RpbGetBucketResp
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

type CommitHook struct {
	Name   string
	ModFun *ModFun
}

type ModFun struct {
	Module   string
	Function string
}

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
	PreCommit     []*CommitHook
	PostCommit    []*CommitHook
	ChashKeyFun   *ModFun
	LinkFun       *ModFun
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

func getFunFrom(rpbModFun *rpbRiak.RpbModFun) *ModFun {
	var modFun *ModFun
	if rpbModFun == nil {
		modFun = nil
	} else {
		modFun = &ModFun{
			Module:   string(rpbModFun.Module),
			Function: string(rpbModFun.Function),
		}
	}
	return modFun
}

func getHooksFrom(rpbHooks []*rpbRiak.RpbCommitHook) []*CommitHook {
	hooks := make([]*CommitHook, len(rpbHooks))
	for i, hook := range rpbHooks {
		commitHook := &CommitHook{
			Name: string(hook.Name),
		}
		if hook.Modfun != nil {
			commitHook.ModFun = &ModFun{
				Module:   string(hook.Modfun.Module),
				Function: string(hook.Modfun.Function),
			}
		}
		hooks[i] = commitHook
	}
	return hooks
}

// StoreBucketProps

type StoreBucketPropsCommand struct {
	CommandImpl
	protobuf *rpbRiak.RpbSetBucketReq
}

func (cmd *StoreBucketPropsCommand) Name() string {
	return "StoreBucketProps"
}

func (cmd *StoreBucketPropsCommand) constructPbRequest() (proto.Message, error) {
	return cmd.protobuf, nil
}

func (cmd *StoreBucketPropsCommand) onSuccess(msg proto.Message) error {
	cmd.Success = true
	return nil
}

func (cmd *StoreBucketPropsCommand) getRequestCode() byte {
	return rpbCode_RpbSetBucketReq
}

func (cmd *StoreBucketPropsCommand) getResponseCode() byte {
	return rpbCode_RpbSetBucketResp
}

func (cmd *StoreBucketPropsCommand) getResponseProtobufMessage() proto.Message {
	return nil
}

type StoreBucketPropsCommandBuilder struct {
	protobuf *rpbRiak.RpbSetBucketReq
	props    *rpbRiak.RpbBucketProps
}

func NewStoreBucketPropsCommandBuilder() *StoreBucketPropsCommandBuilder {
	props := &rpbRiak.RpbBucketProps{}
	protobuf := &rpbRiak.RpbSetBucketReq{
		Props: props,
	}
	builder := &StoreBucketPropsCommandBuilder{protobuf: protobuf, props: props}
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithBucketType(bucketType string) *StoreBucketPropsCommandBuilder {
	builder.protobuf.Type = []byte(bucketType)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithBucket(bucket string) *StoreBucketPropsCommandBuilder {
	builder.protobuf.Bucket = []byte(bucket)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithNVal(nval uint32) *StoreBucketPropsCommandBuilder {
	builder.props.NVal = &nval
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithAllowMult(allowMult bool) *StoreBucketPropsCommandBuilder {
	builder.props.AllowMult = &allowMult
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithLastWriteWins(lww bool) *StoreBucketPropsCommandBuilder {
	builder.props.LastWriteWins = &lww
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithOldVClock(oldVClock uint32) *StoreBucketPropsCommandBuilder {
	builder.props.OldVclock = &oldVClock
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithYoungVClock(youngVClock uint32) *StoreBucketPropsCommandBuilder {
	builder.props.YoungVclock = &youngVClock
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithBigVClock(bigVClock uint32) *StoreBucketPropsCommandBuilder {
	builder.props.BigVclock = &bigVClock
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithSmallVClock(smallVClock uint32) *StoreBucketPropsCommandBuilder {
	builder.props.SmallVclock = &smallVClock
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithR(r uint32) *StoreBucketPropsCommandBuilder {
	builder.props.R = &r
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithPr(pr uint32) *StoreBucketPropsCommandBuilder {
	builder.props.Pr = &pr
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithW(w uint32) *StoreBucketPropsCommandBuilder {
	builder.props.W = &w
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithPw(pw uint32) *StoreBucketPropsCommandBuilder {
	builder.props.Pw = &pw
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithDw(dw uint32) *StoreBucketPropsCommandBuilder {
	builder.props.Dw = &dw
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithRw(rw uint32) *StoreBucketPropsCommandBuilder {
	builder.props.Rw = &rw
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithBasicQuorum(basicQuorum bool) *StoreBucketPropsCommandBuilder {
	builder.props.BasicQuorum = &basicQuorum
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithNotFoundOk(notFoundOk bool) *StoreBucketPropsCommandBuilder {
	builder.props.NotfoundOk = &notFoundOk
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithSearch(search bool) *StoreBucketPropsCommandBuilder {
	builder.props.Search = &search
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithBackend(backend string) *StoreBucketPropsCommandBuilder {
	builder.props.Backend = []byte(backend)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithSearchIndex(searchIndex string) *StoreBucketPropsCommandBuilder {
	builder.props.SearchIndex = []byte(searchIndex)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) AddPreCommit(commitHook *CommitHook) *StoreBucketPropsCommandBuilder {
	rpbCommitHook := toRpbCommitHook(commitHook)
	builder.props.Precommit = addCommitHookTo(builder.props.Precommit, rpbCommitHook)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) AddPostCommit(commitHook *CommitHook) *StoreBucketPropsCommandBuilder {
	rpbCommitHook := toRpbCommitHook(commitHook)
	builder.props.Postcommit = addCommitHookTo(builder.props.Postcommit, rpbCommitHook)
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) WithChashKeyFun(val *ModFun) *StoreBucketPropsCommandBuilder {
	builder.props.ChashKeyfun = &rpbRiak.RpbModFun{
		Module:   []byte(val.Module),
		Function: []byte(val.Function),
	}
	return builder
}

func (builder *StoreBucketPropsCommandBuilder) Build() (Command, error) {
	if builder.protobuf == nil {
		panic("builder.protobuf must not be nil")
	}
	if err := validateLocatable(builder.protobuf); err != nil {
		return nil, err
	}
	return &StoreBucketPropsCommand{protobuf: builder.protobuf}, nil
}

func addCommitHookTo(rpbHooks []*rpbRiak.RpbCommitHook, rpbCommitHook *rpbRiak.RpbCommitHook) []*rpbRiak.RpbCommitHook {
	return append(rpbHooks, rpbCommitHook)
}

func toRpbCommitHook(commitHook *CommitHook) *rpbRiak.RpbCommitHook {
	rpbCommitHook := &rpbRiak.RpbCommitHook{
		Name: []byte(commitHook.Name),
	}
	if commitHook.ModFun != nil {
		rpbCommitHook.Modfun = &rpbRiak.RpbModFun{
			Module:   []byte(commitHook.ModFun.Module),
			Function: []byte(commitHook.ModFun.Function),
		}
	}
	return rpbCommitHook
}
