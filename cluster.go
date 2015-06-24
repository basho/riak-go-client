package riak

type ClusterOptions struct {
	Nodes []*Node
}

type Cluster struct {
	nodes []*Node
}

var defaultClusterOptions = &ClusterOptions{
	Nodes: make([]*Node, 1),
}

func NewCluster(options *ClusterOptions) (*Cluster, error) {
	if options == nil {
		options = defaultClusterOptions
	}
	if len(options.Nodes) == 0 {
		if defaultNode, err := NewNode(nil); err != nil {
			return nil, err
		} else {
			options.Nodes = append(options.Nodes, defaultNode)
		}
	}
	return &Cluster{
		nodes: options.Nodes,
	}, nil
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}
