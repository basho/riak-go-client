package riak

type ClusterOptions struct {
	Nodes       []*Node
	NodeManager NodeManager
}

type Cluster struct {
	nodes       []*Node
	state       clusterState
	nodeManager NodeManager
}

var defaultClusterOptions = &ClusterOptions{
	Nodes:       make([]*Node, 0),
	NodeManager: &defaultNodeManager{},
}

func NewCluster(options *ClusterOptions) (c *Cluster, err error) {
	if options == nil {
		options = defaultClusterOptions
	}

	c = &Cluster{}

	if c.nodes, err = optNodes(options.Nodes); err != nil {
		c = nil
		return
	}

	c.nodeManager = options.NodeManager

	c.state = CLUSTER_CREATED
	return
}

func optNodes(nodes []*Node) (rv []*Node, err error) {
	if nodes == nil {
		nodes = make([]*Node, 0)
	}
	if len(nodes) == 0 {
		var defaultNode *Node
		if defaultNode, err = NewNode(nil); err == nil {
			rv = append(nodes, defaultNode)
		}
	}
	return
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}
