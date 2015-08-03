// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

import (
	"net"
)

type Client struct {
	conn  *connection
	debug bool
}

func New(addrs []string, max int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8098")
	if err != nil {
		return nil, err
	}
	opts := &connectionOptions{remoteAddress: addr}
	conn, err := newConnection(opts)
	if err != nil {
		return nil, err
	}
	client := &Client{
		conn:  conn,
		debug: true,
	}
	return client, nil
}

func (c *Client) Debug(debug bool) {
	c.debug = debug
}
