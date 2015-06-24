package riak

type ClusterOptions struct {
}

type Cluster struct {
}

var defaultClusterOptions = &ClusterOptions{
}

func NewCluster(options *ClusterOptions) (*Cluster, error) {
	if options == nil {
		options = defaultClusterOptions
	}
	return &Cluster{}, nil
}

// exported funcs

func (c *Cluster) String() string {
	// return fmt.Sprintf("%v|%d", c.addr)
	return "TODO cluster"
}
