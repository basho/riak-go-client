package riak

import (
	"fmt"
	"sync"
)

// Constants identifying Cluster state
const (
	clusterError state = iota
	clusterCreated
	clusterRunning
	clusterQueueing
	clusterShuttingDown
	clusterShutdown
)

// ClusterOptions object contains your pool of Node objects and the NodeManager
// If the NodeManager is not defined, the defaultNodeManager is used
type ClusterOptions struct {
	Nodes             []*Node
	NodeManager       NodeManager
	ExecutionAttempts byte
	QueueCommands     bool
	QueueMaxDepth     uint16
}

// Cluster object contains your pool of Node objects, the NodeManager and the
// current stateData object of the cluster
type Cluster struct {
	stateData
	nodes             []*Node
	nodeManager       NodeManager
	executionAttempts byte
	queueCommands     bool
	queueMaxDepth     uint16
}

// Async object is used to
type Async struct {
	Command Command
	Done    chan Command
	Wait    *sync.WaitGroup
	Error   error
}

// Cluster errors
var (
	ErrClusterCommandRequired                 = newClientError("[Cluster] Command must be non-nil")
	ErrClusterAsyncRequiresChannelOrWaitGroup = newClientError("[Cluster] ExecuteAsync argument requires a channel or sync.WaitGroup to indicate completion")
)

var defaultClusterOptions = &ClusterOptions{
	Nodes:             make([]*Node, 0),
	NodeManager:       &defaultNodeManager{},
	ExecutionAttempts: defaultExecutionAttempts,
}

// NewCluster generates a new Cluster object using the provided ClusterOptions object
func NewCluster(options *ClusterOptions) (c *Cluster, err error) {
	if options == nil {
		options = defaultClusterOptions
	}
	if options.NodeManager == nil {
		options.NodeManager = &defaultNodeManager{}
	}
	if options.ExecutionAttempts == 0 {
		options.ExecutionAttempts = defaultExecutionAttempts
	}
	if options.QueueCommands && options.QueueMaxDepth == 0 {
		options.QueueMaxDepth = defaultQueueMaxDepth
	}

	c = &Cluster{}

	if c.nodes, err = optNodes(options.Nodes); err != nil {
		c = nil
		return
	}

	c.nodeManager = options.NodeManager
	c.executionAttempts = options.ExecutionAttempts
	c.queueCommands = options.QueueCommands
	c.queueMaxDepth = options.QueueMaxDepth

	c.setStateDesc("clusterError", "clusterCreated", "clusterRunning", "clusterQueueing", "clusterShuttingDown", "clusterShutdown")
	c.setState(clusterCreated)
	return
}

// String returns a formatted string that lists status information for the Cluster
func (c *Cluster) String() string {
	return fmt.Sprintf("%v", c.nodes)
}

// Start opens connections with your configured nodes and adds them to
// the active pool
func (c *Cluster) Start() (err error) {
	if c.isCurrentState(clusterRunning) {
		logWarnln("[Cluster]", "cluster already running.")
		return
	}
	if err = c.stateCheck(clusterCreated); err != nil {
		return
	}

	logDebug("[Cluster]", "starting")

	for _, node := range c.nodes {
		if err = node.start(); err != nil {
			return
		}
	}

	c.setState(clusterRunning)
	logDebug("[Cluster]", "cluster started")

	return
}

// Stop closes the connections with your configured nodes and removes them from
// the active pool
func (c *Cluster) Stop() (err error) {
	if err = c.stateCheck(clusterRunning, clusterQueueing); err != nil {
		logError("[Cluster]", "Stop: %s", err.Error())
		return
	}

	logDebug("[Cluster]", "shutting down")
	c.setState(clusterShuttingDown)
	for _, node := range c.nodes {
		err = node.stop() // TODO multiple errors?
	}

	allStopped := true
	logDebug("[Cluster]", "checking to see if nodes are shut down")
	for _, node := range c.nodes {
		nodeState := node.getState()
		if nodeState != nodeShutdown {
			allStopped = false
			break
		}
	}

	if allStopped {
		c.setState(clusterShutdown)
		logDebug("[Cluster]", "cluster shut down")
		// TODO queueing check for commands still in the queue
	} else {
		panic("[Cluster] nodes still running when all should be stopped")
	}

	return
}

// Asynchronously execute the provided Command against the active pooled Nodes using the NodeManager
func (c *Cluster) ExecuteAsync(async *Async) (err error) {
	if async.Command == nil {
		return ErrClusterCommandRequired
	}
	if async.Done == nil && async.Wait == nil {
		return ErrClusterAsyncRequiresChannelOrWaitGroup
	}
	if async.Wait != nil {
		async.Wait.Add(1)
	}
	go execute(c, async)
	return nil
}

// Synchronously execute the provided Command against the active pooled Nodes using the NodeManager
func (c *Cluster) Execute(command Command) (err error) {
	if command == nil {
		return ErrClusterCommandRequired
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	async := &Async{
		Command: command,
		Wait:    wg,
	}
	go execute(c, async)
	wg.Wait() // TODO: timeout?
	return nil
}

func execute(c *Cluster, async *Async) {
	executed := false
	command := async.Command
	command.setRemainingTries(c.executionAttempts)
	for command.hasRemainingTries() {
		if executed, async.Error = c.nodeManager.ExecuteOnNode(c.nodes, command, nil); async.Error == nil && executed == true {
			break
		} else {
			// NB: retry since either error occurred *or* command was not executed
			logDebug("[Cluster]", "retrying command '%s'", command.Name())
			command.decrementRemainingTries()
		}
	}
	if async.Done != nil {
		logDebug("[Cluster]", "signaling async.Done with '%s'", command.Name())
		async.Done <- async.Command
	}
	if async.Wait != nil {
		logDebug("[Cluster]", "signaling async.Wait")
		async.Wait.Done()
	}
	// NB: do *not* call command.onError here as it will have been called in connection
	// TODO
	// if !executed and command has remaining tries, queue command?
	// or, reset tries and queue?
	// if queue fails, command.onError(ErrNoNodesAvailable)
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
