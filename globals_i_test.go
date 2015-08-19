// +build integration

package riak

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
)

func integrationTestsBuildCluster() *Cluster {
	var cluster *Cluster
	var err error
	nodeOpts := &NodeOptions{
		RemoteAddress:  getRiakAddress(),
		RequestTimeout: time.Second * 20, // TODO in the future, settable per-request
	}
	var node *Node
	node, err = NewNode(nodeOpts)
	if err != nil {
		panic(fmt.Sprintf("error building integration test node object: %s", err.Error()))
	}
	if node == nil {
		panic("NewNode returned nil!")
	}
	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes: nodes,
	}
	cluster, err = NewCluster(opts)
	if err != nil {
		panic(fmt.Sprintf("error building integration test cluster object: %s", err.Error()))
	}
	if err = cluster.Start(); err != nil {
		panic(fmt.Sprintf("error starting integration test cluster object: %s", err.Error()))
	}
	return cluster
}

func readWritePingResp(t *testing.T, c net.Conn, shouldClose bool) (success bool) {
	success = false
	if err := readClientMessage(c); err != nil {
		if err == io.EOF {
			c.Close()
		} else {
			t.Error(err)
		}
		success = false
		return
	}
	data := buildRiakMessage(rpbCode_RpbPingResp, nil)
	count, err := c.Write(data)
	if err == nil {
		// logDebug("[readWritePingResp]", "wrote message '%v', count '%d'", data, count)
		success = true
	} else {
		t.Error(err)
		success = false
	}
	if count != len(data) {
		t.Errorf("expected to write %v bytes, wrote %v bytes", len(data), count)
		success = false
	}
	if shouldClose {
		c.Close()
	}
	return
}

func readClientMessage(c net.Conn) (err error) {
	sizeBuf := make([]byte, 4)
	var count int = 0
	if count, err = io.ReadFull(c, sizeBuf); err == nil && count == 4 {
		messageLength := binary.BigEndian.Uint32(sizeBuf)
		// logDebug("[readClientMessage]", "read size '%v', count '%d', messageLength '%d'", sizeBuf, count, messageLength)
		data := make([]byte, messageLength)
		count, err = io.ReadFull(c, data)
		if err != nil {
			return
		} else if uint32(count) != messageLength {
			err = fmt.Errorf("[readClientMessage] message length: %d, only read: %d", messageLength, count)
		}
		// logDebug("[readClientMessage]", "read message '%v', count '%d'", data, count)
	} else {
		err = errors.New(fmt.Sprintf("[readClientMessage] error reading command size into sizeBuf: count %d, err %s", count, err))
	}
	return
}

func handleClientMessageWithRiakError(t *testing.T, c net.Conn, msgCount uint16, respChan chan bool) {
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < int(msgCount); i++ {
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
}
