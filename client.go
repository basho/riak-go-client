package riak

import (
	"fmt"
	"strconv"
	"strings"
)

const ErrClientInvalidRemoteAddress = "[Client] invalid RemoteAddress '%s'"

var (
	ErrClientOptionsRequired     = newClientError("[Client] options are required", nil)
	ErrClientMissingRequiredData = newClientError("[Client] options must specify either a Cluster or a set of RemoteAddresses", nil)
)

// Client object contains your cluster object
type Client struct {
	cluster *Cluster
}

// Options for creating a new Client. Either Cluster or Port/RemoteAddress information must be provided
type NewClientOptions struct {
	Cluster         *Cluster
	Port            uint16   // NB: if specified, all connections will use this value if port is not provided
	RemoteAddresses []string // NB: in the form HOST|IP[:PORT]
}

// NewClient generates a new Client object using the provided options
func NewClient(opts *NewClientOptions) (*Client, error) {
	if opts == nil {
		return nil, ErrClientOptionsRequired
	}
	if opts.Cluster != nil {
		return newClientUsingCluster(opts.Cluster)
	}
	if opts.RemoteAddresses != nil {
		return newClientUsingAddresses(opts.Port, opts.RemoteAddresses)
	}
	return nil, ErrClientMissingRequiredData
}

func (c *Client) Cluster() *Cluster {
	return c.cluster
}

// Execute (synchronously) the provided Command against the cluster
func (c *Client) Execute(cmd Command) error {
	return c.cluster.Execute(cmd)
}

// Execute (asynchronously) the provided Command against the cluster
func (c *Client) ExecuteAsync(a *Async) error {
	return c.cluster.ExecuteAsync(a)
}

// Pings the cluster
func (c *Client) Ping() (bool, error) {
	cmd := &PingCommand{}
	err := c.cluster.Execute(cmd)
	return cmd.Success(), err
}

// Stop the nodes in the cluster and the cluster itself
func (c *Client) Stop() error {
	return c.cluster.Stop()
}

func newClientUsingCluster(cluster *Cluster) (*Client, error) {
	if err := cluster.Start(); err != nil {
		return nil, err
	}
	return &Client{
		cluster: cluster,
	}, nil
}

func newClientUsingAddresses(port uint16, remoteAddresses []string) (*Client, error) {
	if len(remoteAddresses) == 0 {
		remoteAddresses = make([]string, 1)
		remoteAddresses[0] = defaultRemoteAddress
	}
	nodes := make([]*Node, len(remoteAddresses))
	for i, ra := range remoteAddresses {
		nopts := &NodeOptions{
			MinConnections: 10,
		}
		s := strings.SplitN(ra, ":", 2)
		switch len(s) {
		case 0:
			return nil, newClientError(fmt.Sprintf(ErrClientInvalidRemoteAddress, ra), nil)
		case 1:
			if port > 0 {
				nopts.RemoteAddress = fmt.Sprintf("%s:%d", s[0], port)
			} else {
				nopts.RemoteAddress = fmt.Sprintf("%s:%d", s[0], defaultRemotePort)
			}
		case 2:
			if p, err := strconv.Atoi(s[1]); err != nil {
				return nil, newClientError(ErrClientInvalidRemoteAddress, err)
			} else {
				nopts.RemoteAddress = fmt.Sprintf("%s:%d", s[0], p)
			}
		default:
			return nil, newClientError(fmt.Sprintf(ErrClientInvalidRemoteAddress, ra), nil)
		}
		if node, err := NewNode(nopts); err != nil {
			return nil, err
		} else {
			nodes[i] = node
		}
	}
	copts := &ClusterOptions{
		Nodes: nodes,
	}
	if cluster, err := NewCluster(copts); err != nil {
		return nil, err
	} else {
		return newClientUsingCluster(cluster)
	}
}

// Fetch is a simple invocation of the NewFetchValueCommand
func (c *Client) Fetch(bucketType, bucket, key string) ([]*Object, error) {
	cmd, err := NewFetchValueCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithKey(key).
		Build()

	if err != nil {
		return nil, err
	}
	if err = c.cluster.Execute(cmd); err != nil {
		return nil, err
	}

	res := cmd.(*FetchValueCommand)
	if res.Error() != nil || res.Response.IsNotFound {
		return nil, res.Error()
	}

	return res.Response.Values, nil
}

// Store is a simple invocation of the StorehValueCommand
func (c *Client) Store(bucketType, bucket, key string, obj *Object) ([]*Object, error) {
	cmd, err := NewStoreValueCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithKey(key).
		WithContent(obj).
		Build()

	if err != nil {
		return nil, err
	}
	if err = c.cluster.Execute(cmd); err != nil {
		return nil, err
	}

	res := cmd.(*StoreValueCommand)
	if res.Error() != nil {
		return nil, res.Error()
	}

	return res.Response.Values, nil
}

// Delete is a simple invocation of the NewFetchValueCommand
func (c *Client) Delete(bucketType, bucket, key string) error {
	cmd, err := NewDeleteValueCommandBuilder().
		WithBucketType(bucketType).
		WithBucket(bucket).
		WithKey(key).
		Build()

	if err != nil {
		return err
	}
	if err = c.cluster.Execute(cmd); err != nil {
		return err
	}

	res := cmd.(*DeleteValueCommand)
	if res.Error() != nil {
		return res.Error()
	}

	return nil
}
