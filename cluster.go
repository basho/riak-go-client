package riak

import (
	"fmt"
	"time"
)

// Constants identifying Cluster state
const (
	clusterCreated state = iota
	clusterRunning
	clusterShuttingDown
	clusterShutdown
	clusterError
)

// ClusterOptions object contains your pool of Node objects and the NodeManager
// If the NodeManager is not defined, the defaultNodeManager is used
type ClusterOptions struct {
	Nodes                  []*Node
	NodeManager            NodeManager
	ExecutionAttempts      byte
	QueueMaxDepth          uint16
	QueueExecutionInterval time.Duration
}

// Cluster object contains your pool of Node objects, the NodeManager and the
// current stateData object of the cluster
type Cluster struct {
	stateData
	stopChan           chan bool
	nodes              []*Node
	nodeManager        NodeManager
	executionAttempts  byte
	queueCommands      bool
	cq                 *queue
	commandQueueTicker *time.Ticker
}

// Cluster errors
var (
	ErrClusterCommandRequired                 = newClientError("[Cluster] Command must be non-nil", nil)
	ErrClusterAsyncRequiresChannelOrWaitGroup = newClientError("[Cluster] ExecuteAsync argument requires a channel or sync.WaitGroup to indicate completion", nil)
	ErrClusterEnqueueWhileShuttingDown        = newClientError("[Cluster] will not enqueue command, shutting down", nil)
	ErrClusterShuttingDown                    = newClientError("[Cluster] will not execute command, shutting down", nil)
	ErrClusterNodeMustBeNonNil                = newClientError("[Cluster] node argument must be non-nil", nil)
)

const ErrClusterNoNodesAvailable = "[Cluster] all retries exhausted and/or no nodes available to execute command"

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

	c = &Cluster{
		executionAttempts: options.ExecutionAttempts,
		nodeManager:       options.NodeManager,
	}
	c.initStateData("clusterCreated", "clusterRunning", "clusterShuttingDown", "clusterShutdown", "clusterError")

	if c.nodes, err = optNodes(options.Nodes); err != nil {
		c = nil
		return
	}

	if options.QueueMaxDepth > 0 {
		if options.QueueExecutionInterval == 0 {
			options.QueueExecutionInterval = defaultQueueExecutionInterval
		}
		c.queueCommands = true
		c.stopChan = make(chan bool)
		c.cq = newQueue(options.QueueMaxDepth)
		c.commandQueueTicker = time.NewTicker(options.QueueExecutionInterval)
		go c.executeEnqueuedCommands()
	}

	c.setState(clusterCreated)
	return
}

// String returns a formatted string that lists status information for the Cluster
func (c *Cluster) String() string {
	return fmt.Sprintf("%v", c.nodes)
}

// Start opens connections with your configured nodes and adds them to
// the active pool
func (c *Cluster) Start() error {
	if c.isCurrentState(clusterRunning) {
		logWarnln("[Cluster]", "cluster already running.")
		return nil
	}

	if err := c.stateCheck(clusterCreated); err != nil {
		return err
	}

	logDebug("[Cluster]", "starting")

	for _, node := range c.nodes {
		if err := node.start(); err != nil {
			return err
		}
	}

	c.setState(clusterRunning)
	logDebug("[Cluster]", "cluster started")

	return nil
}

// Stop closes the connections with your configured nodes and removes them from
// the active pool
func (c *Cluster) Stop() (err error) {
	if err = c.stateCheck(clusterRunning); err != nil {
		return
	}

	logDebug("[Cluster]", "shutting down")

	c.setState(clusterShuttingDown)

	if c.queueCommands {
		c.stopChan <- true
		close(c.stopChan)
		c.commandQueueTicker.Stop()
		qc := c.cq.count()
		if qc > 0 {
			logWarn("[Cluster]", "commands in queue during shutdown: %d", qc)
			var f = func(v interface{}) (bool, bool) {
				if v == nil {
					return true, false
				}
				if a, ok := v.(*Async); ok {
					a.done(ErrClusterShuttingDown)
				}
				return false, false
			}
			if qerr := c.cq.iterate(f); qerr != nil {
				logErr("[Cluster]", qerr)
			}
		}
		c.cq.destroy()
	}

	for _, node := range c.nodes {
		err = node.stop()
		if err != nil {
			logErr("[Cluster]", err)
		}
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
	} else {
		panic("[Cluster] nodes still running when all should be stopped")
	}

	return
}

// Adds a node to the cluster and starts it
func (c *Cluster) AddNode(n *Node) error {
	if n == nil {
		return ErrClusterNodeMustBeNonNil
	}
	for _, node := range c.nodes {
		if n == node {
			return nil
		}
	}
	if c.isCurrentState(clusterRunning) {
		if err := n.start(); err != nil {
			return err
		}
	}
	c.nodes = append(c.nodes, n)
	return nil
}

// Stops the node and removes from the cluster
func (c *Cluster) RemoveNode(n *Node) error {
	if n == nil {
		return ErrClusterNodeMustBeNonNil
	}
	cn := c.nodes
	for i, node := range c.nodes {
		if n == node {
			l := len(cn) - 1
			cn[i], cn[l], c.nodes = cn[l], nil, cn[:l]
			if !node.isCurrentState(nodeCreated) {
				if err := node.stop(); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return nil
}

// Execute (asynchronously) the provided Command against the active pooled Nodes using the NodeManager
func (c *Cluster) ExecuteAsync(async *Async) error {
	if async.Command == nil {
		return ErrClusterCommandRequired
	}
	if async.Done == nil && async.Wait == nil {
		return ErrClusterAsyncRequiresChannelOrWaitGroup
	}
	if async.Wait != nil {
		async.Wait.Add(1)
	}
	go c.execute(async)
	return nil
}

// Execute (synchronously) the provided Command against the active pooled Nodes using the NodeManager
func (c *Cluster) Execute(command Command) error {
	if command == nil {
		return ErrClusterCommandRequired
	}
	async := &Async{
		Command: command,
	}
	c.execute(async)
	if async.Error != nil {
		return async.Error
	}
	if cerr := command.Error(); cerr != nil {
		return cerr
	}
	return nil
}

// NB: will be executed in a goroutine
func (c *Cluster) execute(async *Async) {
	if c == nil {
		panic("[Cluster] nil cluster argument")
	}
	if async == nil {
		panic("[Cluster] nil async argument")
	}
	var err error
	executed := false
	enqueued := false
	command := async.Command
	command.setRemainingTries(c.executionAttempts)
	async.onExecute()
	for command.hasRemainingTries() {
		if err = c.stateCheck(clusterRunning); err != nil {
			break
		}
		executed, err = c.nodeManager.ExecuteOnNode(c.nodes, command, command.getLastNode())
		// NB: do *not* call command.onError here as it will have been called in connection
		if executed {
			// NB: "executed" means that a node sent the data to Riak and received a response
			if err == nil {
				// No need to re-try
				logDebug("[Cluster]", "successfully executed command '%s'", command.Name())
				break
			} else {
				// NB: retry since error occurred
				logDebug("[Cluster]", "executed command '%s': re-try due to error '%v'", command.Name(), err)
			}
		} else {
			// Command did NOT execute
			if err == nil {
				logDebug("[Cluster]", "did NOT execute command '%s', nil err", command.Name())
				// Command did not execute but there was no error, so enqueue it
				if c.queueCommands {
					if err = c.enqueueCommand(async); err == nil {
						enqueued = true
					}
					break
				}
			} else {
				// NB: retry since error occurred
				logDebug("[Cluster]", "did NOT execute command '%s': re-try due to error '%v'", command.Name(), err)
			}
		}

		command.decrementRemainingTries()

		if command.hasRemainingTries() {
			async.onRetry()
		} else {
			err = newClientError(ErrClusterNoNodesAvailable, err)
		}
	}
	if !enqueued {
		async.done(err)
	}
}

func (c *Cluster) enqueueCommand(async *Async) error {
	var err error
	if c.isStateLessThan(clusterShuttingDown) {
		command := async.Command
		logDebug("[Cluster]", "enqueuing command '%s'", command.Name())
		async.onEnqueued()
		err = c.cq.enqueue(async)
		if err != nil {
			async.done(err)
		}
	} else {
		err = ErrClusterEnqueueWhileShuttingDown
		async.done(err)
	}
	return err
}

func (c *Cluster) executeEnqueuedCommands() {
	logDebug("[Cluster]", "(%v) command queue routine is starting", c)
	for {
		select {
		case <-c.stopChan:
			logDebug("[Cluster]", "(%v) command queue routine is quitting", c)
			return
		case t := <-c.commandQueueTicker.C:
			// NB: ensure we're not already shutting down
			if c.isStateLessThan(clusterShuttingDown) {
				var f = func(v interface{}) (bool, bool) {
					if !c.isStateLessThan(clusterShuttingDown) {
						logDebug("[Cluster]", "(%v) shutting down, command queue routine is quitting")
						return true, false
					}
					if v == nil {
						return true, false
					}
					var re_enqueue bool
					async := v.(*Async)
					if t.After(async.executeAt) {
						re_enqueue = false
						logDebug("[Cluster]", "(%v) executing queued command '%s' at %v", c, async.Command.Name(), t)
						go c.execute(async) // NB: *may* re-enqueue, so goroutine required
					} else {
						re_enqueue = true
						logDebug("[Cluster]", "(%v) skipping queued command '%s'", c, async.Command.Name())
					}
					return false, re_enqueue
				}
				if qerr := c.cq.iterate(f); qerr != nil {
					logErr("[Cluster]", qerr)
				}
			} else {
				logDebug("[Cluster]", "(%v) shutting down, command queue routine is quitting")
				return
			}
		}
	}
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
