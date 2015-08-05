/*
Package riak provides the interfaces needed to interact with Riak KV using
Protocol Buffers. Riak KV is a distributed key-value datastore designed to be
fault tolerant, scalable, and flexible.

Currently, this library was written for and tested against Riak KV 2.0+.

TL;DR;

  import "fmt"

  func main() {
    nodeOpts := &NodeOptions{
      RemoteAddress: "127.0.0.1:8098",
    }

    var node *Node
    var err error
    if node, err = NewNode(nodeOpts); err != nil {
      fmt.Println(err.Error())
    }

    nodes := []*Node{node}
    opts := &ClusterOptions{
      Nodes: nodes,
    }

    cluster, err := NewCluster(opts)
    if err != nil {
      fmt.Println(err.Error())
    }

    defer func() {
      if err := cluster.Stop(); err != nil {
        fmt.Println(err.Error())
      }
    }()

    if err := cluster.Start(); err != nil {
      fmt.Println(err.Error())
    }

    obj := getBasicObject()

    cmd, err := NewStoreValueCommandBuilder().
  		WithBucket(testBucketName).
  		WithContent(obj).
  		Build()
    if err != nil {
  		fmt.Println(err.Error())
  	}

  	if err := cluster.Execute(cmd); err != nil {
  		fmt.Println(err.Error())
  	}

  	if svc, ok := cmd.(*StoreValueCommand); ok {
  		rsp := svc.Response
      fmt.Println(rsp.GeneratedKey)
  	} else {
  		fmt.Println(err.Error())
  	}
  }
*/
package riak
