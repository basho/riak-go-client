package riak

import (
	"sync"
)

type ClusterOptions struct {
	Nodes       []*Node
	NodeManager NodeManager
}

type Cluster struct {
	nodes       []*Node
	nodeManager NodeManager

	// Cluster State
	stateMtx sync.RWMutex
	state    clusterState
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

	c.state = CLUSTER_CREATED
	return
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}

func (c *Cluster) Start(options *ClusterOptions) (err error) {
	/*
			if c.currentState(CLUSTER_RUNNING) {
		        logWarn("[Cluster] cluster already running.")
				return
			}

			if err = n.stateCheck(CLUSTER_CREATED); err == nil {
		        logDebug("[Cluster] starting.")

				for _, node := range c.nodes {
					if err = node.Start(); err != nil {
						return
					}
				}

				c.setState(CLUSTER_RUNNING)
		        logDebug("[Cluster] cluster started.")
			}
	*/

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
