// +build integration

package riak

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"

	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
)

func init() {
	runtime.GOMAXPROCS(2)
}

func readWritePingResp(t *testing.T, c net.Conn) (success bool) {
	success = false
	if err := readClientMessage(c); err != nil {
		t.Error(err)
	}
	data := buildRiakMessage(rpbCode_RpbPingResp, nil)
	count, err := c.Write(data)
	if err != nil {
		t.Error(err)
	}
	if count != len(data) {
		t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
	}
	c.Close()
	success = true
	return
}

func readClientMessage(c net.Conn) (err error) {
	sizeBuf := make([]byte, 4)
	var count int = 0
	if count, err = io.ReadFull(c, sizeBuf); err == nil && count == 4 {
		messageLength := binary.BigEndian.Uint32(sizeBuf)
		data = make([]byte, messageLength)
		count, err = io.ReadFull(c, data)
		if err != nil {
			return
		} else if uint32(count) != messageLength {
			err = fmt.Errorf("[readClientMessage] message length: %d, only read: %d", messageLength, count)
		}
	} else {
		err = errors.New("error reading command size into sizeBuf")
	}
	return
}

func handleClientMessageWithRiakError(t *testing.T, c net.Conn, respChan chan bool) {
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()

	if err := readClientMessage(c); err != nil {
		t.Error(err)
	}

	var errcode uint32 = 1
	errmsg := bytes.NewBufferString("this is an error")
	rpbErr := &rpb_riak.RpbErrorResp{
		Errcode: &errcode,
		Errmsg:  errmsg.Bytes(),
	}
	encoded, err := proto.Marshal(rpbErr)
	if err != nil {
		t.Error(err)
	}
	data := buildRiakMessage(rpbCode_RpbErrorResp, encoded)
	count, err := c.Write(data)
	if err != nil {
		t.Error(err)
	}
	if count != len(data) {
		t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
	}
	if respChan != nil {
		respChan <- true
	}
}
