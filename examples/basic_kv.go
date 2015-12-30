package main

import (
	"fmt"

	riak "github.com/basho/riak-go-client"
)

// make sure these bucket-types are created:
// 'animals', 'quotes', 'sports', 'cars', 'users', 'n_val_of_5'
func main() {
	//riak.EnableDebugLogging = true

	nodeOpts := &riak.NodeOptions{
		RemoteAddress: "riak-test:10017",
	}

	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		fmt.Println(err.Error())
	}

	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := riak.NewCluster(opts)
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

	// ping
	ping := &riak.PingCommand{}
	if err := cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ping passed")
	}

	fetchRufus(cluster)
	storeRufus(cluster)
	storeStray(cluster)
	fetchRufusWithR(cluster)
	storeQuote(cluster)
	storeAndUpdateSport(cluster)
	storeCar(cluster)
	storeUserThenDelete(cluster)
}

func storeRufus(cluster *riak.Cluster) {
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("WOOF!"),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("animals").
		WithBucket("dogs").
		WithKey("rufus").
		WithContent(obj).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc := cmd.(*riak.StoreValueCommand)
	rsp := svc.Response
	fmt.Println(rsp.VClock)
}

func storeQuote(cluster *riak.Cluster) {
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("I have nothing to declare but my genius"),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("quotes").
		WithBucket("oscar_wilde").
		WithKey("genius").
		WithContent(obj).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc := cmd.(*riak.StoreValueCommand)
	rsp := svc.Response
	fmt.Println(rsp.VClock)
}

func storeCar(cluster *riak.Cluster) {
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("vroom"),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("cars").
		WithBucket("dodge").
		WithKey("viper").
		WithW(3).
		WithContent(obj).
		WithReturnBody(true).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func storeUserThenDelete(cluster *riak.Cluster) {
	obj := &riak.Object{
		ContentType:     "application/json",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("{'user':'data'}"),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("users").
		WithBucket("random_user_keys").
		WithContent(obj).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc := cmd.(*riak.StoreValueCommand)
	rsp := svc.Response
	fmt.Printf("Generated key: %v\n", rsp.GeneratedKey)

	cmd, err = riak.NewDeleteValueCommandBuilder().
		WithBucketType("users").
		WithBucket("random_user_keys").
		WithKey(rsp.GeneratedKey).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func storeAndUpdateSport(cluster *riak.Cluster) {
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("Washington Generals"),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("sports").
		WithBucket("nba").
		WithKey("champion").
		WithContent(obj).
		WithReturnBody(true).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc := cmd.(*riak.StoreValueCommand)
	rsp := svc.Response
	obj = rsp.Values[0]
	obj.Value = []byte("Harlem Globetrotters")

	cmd, err = riak.NewStoreValueCommandBuilder().
		WithBucketType("sports").
		WithBucket("nba").
		WithKey("champion").
		WithContent(obj).
		WithReturnBody(true).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc = cmd.(*riak.StoreValueCommand)
	rsp = svc.Response
	obj = rsp.Values[0]
	fmt.Printf("champion: %v\n", string(obj.Value))
}

func fetchRufus(cluster *riak.Cluster) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType("animals").
		WithBucket("dogs").
		WithKey("rufus").
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	fvc := cmd.(*riak.FetchValueCommand)
	rsp := fvc.Response
	fmt.Println(rsp.IsNotFound)
}

func storeStray(cluster *riak.Cluster) {
	// There is not a default content type for an object, so it must be defined. For JSON content
	// types, please use 'application/json'
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("Found in alley."),
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("animals").
		WithBucket("dogs").
		WithContent(obj).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	svc := cmd.(*riak.StoreValueCommand)
	rsp := svc.Response
	fmt.Println(rsp.GeneratedKey)
}

// https://godoc.org/github.com/basho/riak-go-client#FetchValueCommandBuilder.WithR
func fetchRufusWithR(cluster *riak.Cluster) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType("animals").
		WithBucket("dogs").
		WithKey("rufus").
		WithR(3).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	fvc := cmd.(*riak.FetchValueCommand)
	rsp := fvc.Response
	fmt.Println(rsp.IsNotFound)
}

func fetchChampion(cluster *riak.Cluster) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType("sports").
		WithBucket("nba").
		WithKey("champion").
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}

	fvc := cmd.(*riak.FetchValueCommand)
	rsp := fvc.Response
	var obj *riak.Object
	if len(rsp.Values) > 0 {
		obj = rsp.Values[0]
	} else {
		obj = &riak.Object{
			ContentType:     "text/plain",
			Charset:         "utf-8",
			ContentEncoding: "utf-8",
			Value:           nil,
		}
	}

	obj.Value = []byte("Harlem Globetrotters")

	cmd, err = riak.NewStoreValueCommandBuilder().
		WithBucketType("sports").
		WithBucket("nba").
		WithContent(obj).
		Build()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := cluster.Execute(cmd); err != nil {
		fmt.Println(err.Error())
		return
	}
}
