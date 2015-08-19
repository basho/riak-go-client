package riak

import (
	"fmt"
	"sync"
	"time"
)

// Constants identifying Cluster state
const (
	clusterError state = iota
	clusterCreated
	clusterRunning
	clusterShuttingDown
	clusterShutdown
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
	commandQueue       *queue
	commandQueueTicker *time.Ticker
}

// Async object is used to pass required arguments to execute a Command asynchronously
type Async struct {
	Command Command
	Done    chan Command
	Wait    *sync.WaitGroup
	Error   error
}

func (a *Async) done(err error) {
	if err != nil {
		logErr("[Async]", err)
		a.Error = err
	}
	if a.Done != nil {
		logDebug("[Cluster]", "signaling a.Done channel with '%s'", a.Command.Name())
		a.Done <- a.Command
	}
	if a.Wait != nil {
		logDebug("[Cluster]", "signaling a.Wait WaitGroup for '%s'", a.Command.Name())
		a.Wait.Done()
	}
}

// Cluster errors
var (
	ErrClusterCommandRequired                 = newClientError("[Cluster] Command must be non-nil")
	ErrClusterAsyncRequiresChannelOrWaitGroup = newClientError("[Cluster] ExecuteAsync argument requires a channel or sync.WaitGroup to indicate completion")
	ErrClusterNoNodesAvailable                = newClientError("[Cluster] no nodes available to execute command")
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

	c = &Cluster{
		executionAttempts: options.ExecutionAttempts,
		nodeManager:       options.NodeManager,
	}
	c.initStateData("clusterError", "clusterCreated", "clusterRunning", "clusterShuttingDown", "clusterShutdown")

	if c.nodes, err = optNodes(options.Nodes); err != nil {
		c = nil
		return
	}
	c.nodeManager.Init(c.nodes)

	if options.QueueMaxDepth > 0 {
		if options.QueueExecutionInterval == 0 {
			options.QueueExecutionInterval = defaultQueueExecutionInterval
		}
		c.queueCommands = true
		c.stopChan = make(chan bool)
		c.commandQueue = newQueue(options.QueueMaxDepth)
		// TODO configurable queue submit interval?
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
	if err = c.stateCheck(clusterRunning); err != nil {
		logError("[Cluster]", "Stop: %s", err.Error())
		return
	}
	c.setState(clusterShuttingDown)
	if c.queueCommands {
		c.stopChan <- true
		close(c.stopChan)
		c.commandQueueTicker.Stop()
		c.commandQueue.destroy()
	}
	logDebug("[Cluster]", "shutting down")

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
	go c.execute(async)
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
	go c.execute(async)
	wg.Wait() // TODO: timeout?
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
	for command.hasRemainingTries() {
		if err = c.stateCheck(clusterRunning); err != nil {
			break
		}
		executed, err = c.nodeManager.ExecuteOnNode(command, command.getLastNode())
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
				command.decrementRemainingTries()
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
				} else {
					err = ErrClusterNoNodesAvailable
				}
				break
			} else {
				// NB: retry since error occurred
				logDebug("[Cluster]", "did NOT execute command '%s': re-try due to error '%v'", command.Name(), err)
				command.decrementRemainingTries()
			}
		}
	}
	if !enqueued {
		async.done(err)
	}
}

func (c *Cluster) enqueueCommand(async *Async) error {
	// if queue fails, command.onError(ErrNoNodesAvailable)
	command := async.Command
	logDebug("[Cluster]", "enqueuing command '%s'", command.Name())
	return c.commandQueue.enqueue(async)
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
			if tmpErr := c.stateCheck(clusterShuttingDown); tmpErr == nil {
				logDebug("[Cluster]", "(%v) shutting down, command queue routine is quitting")
				return
			} else {
				if value, err := c.commandQueue.dequeue(); err != nil {
					async := value.(*Async)
					async.done(err)
				} else {
					if tmpErr := c.stateCheck(clusterShuttingDown); tmpErr == nil {
						logDebug("[Cluster]", "(%v) shutting down, command queue routine is quitting")
						return
					} else {
						async := value.(*Async)
						logDebug("[Cluster]", "(%v) executing queued command at %v", c, t)
						c.execute(async)
					}
				}
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
