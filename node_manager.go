package riak

type NodeManager interface {
	ExecuteOnNode(nodes []*Node, command Command) (executing bool, err error)
}

type defaultNodeManager struct {
}

func (nm *defaultNodeManager) ExecuteOnNode(nodes []*Node, command Command) (executing bool, err error) {
	return true, nil
}
