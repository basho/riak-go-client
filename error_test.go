package riak

import (
	"bytes"
	"testing"

	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
)

func TestBuildRiakErrorFromRpbErrorResp(t *testing.T) {
	var errcode uint32 = 1
	errmsg := bytes.NewBufferString("this is an error")
	rpbErr := &rpb_riak.RpbErrorResp{
		Errcode: &errcode,
		Errmsg:  errmsg.Bytes(),
	}
	err := newRiakError(rpbErr)
	if riakError, ok := err.(RiakError); ok == true {
		if expected, actual := errcode, riakError.Errcode; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "this is an error", riakError.Errmsg; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "RiakError|1|this is an error", riakError.Error(); expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.Error("error in type conversion")
	}
}
