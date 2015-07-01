package riak

import "errors"

// Cluster states

const (
	CLUSTER_ERROR state = iota
	CLUSTER_CREATED
	CLUSTER_RUNNING
	CLUSTER_QUEUEING
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

	c.setStateDesc("CLUSTER_ERROR", "CLUSTER_CREATED", "CLUSTER_RUNNING", "CLUSTER_QUEUEING", "CLUSTER_SHUTTING_DOWN", "CLUSTER_SHUTDOWN")
	c.setState(CLUSTER_CREATED)
	return
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}

func (c *Cluster) Start() (err error) {
	if c.isCurrentState(CLUSTER_RUNNING) {
		logWarnln("[Cluster] cluster already running.")
		return
	}
	if err = c.stateCheck(CLUSTER_CREATED); err != nil {
		return
	} else {
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

func (c *Cluster) Stop() (err error) {
	if err = c.stateCheck(CLUSTER_RUNNING, CLUSTER_QUEUEING); err != nil {
		return
	} else {
		logDebug("[Cluster] shutting down")
		c.setState(CLUSTER_SHUTTING_DOWN)
		for _, node := range c.nodes {
			err = node.Stop() // TODO multiple errors?
		}

		allStopped := true
		logDebug("[Cluster] checking to see if nodes are shut down")
		for _, node := range c.nodes {
			nodeState := node.getState()
			if nodeState != NODE_SHUTDOWN {
				allStopped = false
				break
			}
		}

		if allStopped {
			c.setState(CLUSTER_SHUTDOWN)
			logDebug("[Cluster] cluster shut down")
			/* TODO
			if (this._commandQueue.length) {
				logger.warn('[RiakCluster] There were %d commands in the queue at shutdown', 
					this._commandQueue.length);
			}
			*/
		} else {
			// TODO is this even possible?
			logDebug("[Cluster] nodes still running")
			/*
			var self = this;
			setTimeout(function() {
				self._shutdown();
			}, 1000);
			*/
		}
	}
	return
}

func (c *Cluster) Execute(command Command) (err error) {
	// TODO retries
	// TODO command queueing
	// TODO "previous" node
	executed := false
	if executed, err = c.nodeManager.ExecuteOnNode(c.nodes, command, nil); err != nil {
		return
	}

    if (!executed) {
		err = errors.New("No nodes available to execute command.")
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
