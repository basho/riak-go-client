package riak

import "errors"

// Cluster states constants
const (
	CLUSTER_ERROR state = iota
	CLUSTER_CREATED
	CLUSTER_RUNNING
	CLUSTER_QUEUEING
	CLUSTER_SHUTTING_DOWN
	CLUSTER_SHUTDOWN
)

// ClusterOptions object contains your pool of Node objects and the NodeManager
// If the NodeManager is not defined, the defaultNodeManager is used
type ClusterOptions struct {
	Nodes       []*Node
	NodeManager NodeManager
}

// Cluster object contains your pool of Node objects, the NodeManager and the
// current stateData object of the cluster
type Cluster struct {
	nodes       []*Node
	nodeManager NodeManager
	stateData
}

var defaultClusterOptions = &ClusterOptions{
	Nodes:       make([]*Node, 0),
	NodeManager: &defaultNodeManager{},
}

// NewCluster generates a new Cluster object using the provided ClusterOptions object
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

// TODO
func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}

// Start opens connections with your configured nodes and adds them to
// the active pool
func (c *Cluster) Start() (err error) {
	if c.isCurrentState(CLUSTER_RUNNING) {
		logWarnln("[Cluster] cluster already running.")
		return
	}
	if err = c.stateCheck(CLUSTER_CREATED); err != nil {
		return
	}

	logDebug("[Cluster] starting.")

	for _, node := range c.nodes {
		if err = node.Start(); err != nil {
			return
		}
	}

	c.setState(CLUSTER_RUNNING)
	logDebug("[Cluster] cluster started.")

	return
}

// Stop closes the connections with your configured nodes and removes them from
// the active pool
func (c *Cluster) Stop() (err error) {
	if err = c.stateCheck(CLUSTER_RUNNING, CLUSTER_QUEUEING); err != nil {
		return
	}

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

	return
}

// Execute the provided Command against the active pooled Nodes using the
// NodeManager
func (c *Cluster) Execute(command Command) (err error) {
	// TODO retries
	// TODO command queueing
	// TODO "previous" node
	executed := false
	if executed, err = c.nodeManager.ExecuteOnNode(c.nodes, command, nil); err != nil {
		return
	}

	if !executed {
		err = errors.New("No nodes available to execute command.")
	}

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
	} else {
		rv = nodes
	}
	return
}
