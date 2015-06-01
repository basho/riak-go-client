package riak

type Client struct {
	debug bool
}

func NewClient(addrs []string, max int) *Client {
	client := &Client{
		debug: true,
	}
	return client
}

func (c *Client) Debug(debug bool) {
	c.debug = debug
}
