package riak

type Client struct {
	conn  *Connection
	debug bool
}

func New(addrs []string, max int) (*Client, error) {
	opts := &ConnectionOptions{RemoteAddress: "127.0.0.1:8098"}
	conn, err := NewConnection(opts)
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
