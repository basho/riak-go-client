package riak

// Cluster states

const (
	CLUSTER_ERROR state = iota
	CLUSTER_CREATED
	CLUSTER_RUNNING
	CLUSTER_QUEUING
	CLUSTER_SHUTTING_DOWN
	CLUSTER_SHUTDOWN
)

type ClusterOptions struct {
	Nodes       []*Node
	NodeManager NodeManager
}

type Cluster struct {
	nodes       []*Node
	nodeManager NodeManager
	stateData
}

var defaultClusterOptions = &ClusterOptions{
	Nodes:       make([]*Node, 0),
	NodeManager: &defaultNodeManager{},
}

func NewCluster(options *ClusterOptions) (c *Cluster, err error) {
	if options == nil {
		options = defaultClusterOptions
	}
	if options.NodeManager == nil {
		options.NodeManager = &defaultNodeManager{}
	}

	c = &Cluster{}

	if c.nodes, err = optNodes(options.Nodes); err != nil {
		c = nil
		return
	}

	c.nodeManager = options.NodeManager

	c.setStateDesc("CLUSTER_ERROR", "CLUSTER_CREATED", "CLUSTER_RUNNING", "CLUSTER_QUEUING", "CLUSTER_SHUTTING_DOWN", "CLUSTER_SHUTDOWN")
	c.setState(CLUSTER_CREATED)
	return
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}

func (c *Cluster) Start(options *ClusterOptions) (err error) {
	if c.isCurrentState(CLUSTER_RUNNING) {
		logWarnln("[Cluster] cluster already running.")
		return
	}
	if c.isCurrentState(CLUSTER_CREATED) {
		logDebug("[Cluster] starting.")

		for _, node := range c.nodes {
			if err = node.Start(); err != nil {
				return
			}
		}

		c.setState(CLUSTER_RUNNING)
		logDebug("[Cluster] cluster started.")
	}
	return
}

// non-exported funcs

func optNodes(nodes []*Node) (rv []*Node, err error) {
	if nodes == nil {
		nodes = make([]*Node, 0)
	}
	if len(nodes) == 0 {
		var defaultNode *Node
		if defaultNode, err = NewNode(nil); err == nil {
			rv = append(nodes, defaultNode)
		}
	} else {
		rv = nodes
	}
	return
}
