package riak

import (
	"fmt"

	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
)

type RiakError struct {
	Errcode uint32
	Errmsg  string
}

func newRiakError(rpb *rpb_riak.RpbErrorResp) (e error) {
	return RiakError{
		Errcode: rpb.GetErrcode(),
		Errmsg:  string(rpb.GetErrmsg()),
	}
}

func maybeRiakError(data []byte) (err error) {
	rpbMsgCode := data[0]
	if rpbMsgCode == rpbCode_RpbErrorResp {
		rpb := &rpb_riak.RpbErrorResp{}
		err = proto.Unmarshal(data[1:], rpb)
		if err == nil {
			// No error in Unmarshal, so construct RiakError
			err = newRiakError(rpb)
		}
	}
	return
}

func (e RiakError) Error() (s string) {
	return fmt.Sprintf("RiakError|%d|%s", e.Errcode, e.Errmsg)
}

// Client errors
var (
	ErrAddressRequired      = newClientError("RemoteAddress is required in options")
	ErrAuthMissingConfig    = newClientError("[Connection] authentication is missing TLS config")
	ErrAuthTLSUpgradeFailed = newClientError("[Connection] upgrading to TLS connection failed")
	ErrBucketRequired       = newClientError("Bucket is required")
	ErrKeyRequired          = newClientError("Key is required")
	ErrNilOptions           = newClientError("[Command] options must be non-nil")
	ErrOptionsRequired      = newClientError("Options are required")
	ErrNoNodesAvailable     = newClientError("No nodes available to execute command, or exhausted all tries")
	ErrZeroLength           = newClientError("[Command] 0 byte data response")
)

type ClientError struct {
	Errmsg string
}

func newClientError(errmsg string) error {
	return ClientError{
		Errmsg: errmsg,
	}
}

func (e ClientError) Error() (s string) {
	return fmt.Sprintf("ClientError|%s", e.Errmsg)
}
