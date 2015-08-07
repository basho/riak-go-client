package riak

import (
	"net"
)

// Client object contains your active connection to Riak and the debug flag
type Client struct {
	conn  *connection
	debug bool
}

// New generates a new Client object using an address string in the form of
// "127.0.0.1:8098" with default connectionOptions
func New(addrs string) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", addrs)
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

// Debug controls the debug flag for the Client object, allowing debug messages
// to be written to the logs
func (c *Client) Debug(debug bool) {
	c.debug = debug
}
