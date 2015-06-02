package riak

import (
	"github.com/basho/riak-go-client/core"
)

type Client struct {
	conn *core.Connection
	debug bool
}

func New(addrs []string, max int) (*Client, error) {
	conn, err := core.NewConnection("127.0.0.1:8098")
	if err != nil {
		return nil, err
	}
	client := &Client{
		conn: conn,
		debug: true,
	}
	return client, nil
}

func (c *Client) Debug(debug bool) {
	c.debug = debug
}
