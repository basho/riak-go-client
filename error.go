package riak

import (
	"fmt"
	rpb_riak "github.com/basho-labs/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
)

type Error struct {
	Errcode uint32
	Errmsg  string
}

func newError(rpb *rpb_riak.RpbErrorResp) (e error) {
	return Error{
		Errcode: rpb.GetErrcode(),
		Errmsg:  string(rpb.GetErrmsg()),
	}
}

func maybeRiakError(data []byte) (e error) {
	e = nil
	rpbMsgCode := data[0]
	if rpbMsgCode == rpbCode_RpbErrorResp {
		rpb := &rpb_riak.RpbErrorResp{}
		e = proto.Unmarshal(data[1:], rpb)
		if e == nil {
			e = newError(rpb)
		}
	}
	return
}

func (e Error) Error() (s string) {
	return fmt.Sprintf("%d:%s", e.Errcode, e.Errmsg)
}
