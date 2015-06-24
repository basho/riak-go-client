package riak

// Cluster states

type clusterState byte

const (
	CLUSTER_ERROR clusterState = iota
	CLUSTER_CREATED
	CLUSTER_RUNNING
	CLUSTER_QUEUING
	CLUSTER_SHUTTING_DOWN
	CLUSTER_SHUTDOWN
)

func (v clusterState) String() (rv string) {
	switch v {
	case CLUSTER_CREATED:
		rv = "CLUSTER_CREATED"
	case CLUSTER_RUNNING:
		rv = "CLUSTER_RUNNING"
	case CLUSTER_QUEUING:
		rv = "CLUSTER_QUEUING"
	case CLUSTER_SHUTTING_DOWN:
		rv = "CLUSTER_SHUTTING_DOWN"
	case CLUSTER_SHUTDOWN:
		rv = "CLUSTER_SHUTDOWN"
	}
	return
}

// Node states

type nodeState byte

const (
	NODE_ERROR nodeState = iota
	NODE_CREATED
	NODE_RUNNING
	NODE_HEALTH_CHECKING
	NODE_SHUTTING_DOWN
	NODE_SHUTDOWN
)

func (v nodeState) String() (rv string) {
	switch v {
	case NODE_CREATED:
		rv = "NODE_CREATED"
	case NODE_RUNNING:
		rv = "NODE_RUNNING"
	case NODE_HEALTH_CHECKING:
		rv = "NODE_HEALTH_CHECKING"
	case NODE_SHUTTING_DOWN:
		rv = "NODE_SHUTTING_DOWN"
	case NODE_SHUTDOWN:
		rv = "NODE_SHUTDOWN"
	}
	return
}
