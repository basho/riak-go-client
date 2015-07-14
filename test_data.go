package riak

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

var cluster *Cluster

var riakHost = "riak-test"
var riakPort uint16 = 10017
var remoteAddress = "riak-test:10017"

var vclock = bytes.NewBufferString("vclock123456789")
var vclockBytes = vclock.Bytes()

/*
 * Test bucket type
 *
 * Please create the type 'leveldb_type' to use this:
 *
 * riak-admin bucket-type create leveldb_type '{"props":{"backend":"leveldb_backend"}}'
 * riak-admin bucket-type activate leveldb_type
 */
var testBucketType = "leveldb_type"
var testBucketName = "riak_index_tests"

func init() {
	if hostEnvVar := os.ExpandEnv("$RIAK_HOST"); hostEnvVar != "" {
		riakHost = hostEnvVar
	}
	if portEnvVar := os.ExpandEnv("$RIAK_PORT"); portEnvVar != "" {
		if portNum, err := strconv.Atoi(portEnvVar); err == nil {
			riakPort = uint16(portNum)
		}
	}
	remoteAddress = fmt.Sprintf("%s:%d", riakHost, riakPort)
}

func integrationTestsBuildCluster() {
	var err error
	if cluster == nil {
		nodeOpts := &NodeOptions{
			RemoteAddress:  remoteAddress,
			RequestTimeout: time.Second * 10,
		}
		var node *Node
		node, err = NewNode(nodeOpts)
		if err != nil {
			panic(fmt.Sprintf("error building integration test node object: %s", err.Error()))
		}
		if node == nil {
			panic("NewNode returned nil!")
		}
		nodes := []*Node{node}
		opts := &ClusterOptions{
			Nodes: nodes,
		}
		cluster, err = NewCluster(opts)
		if err != nil {
			panic(fmt.Sprintf("error building integration test cluster object: %s", err.Error()))
		}
		if err = cluster.Start(); err != nil {
			panic(fmt.Sprintf("error starting integration test cluster object: %s", err.Error()))
		}
	}
}

func getBasicObject() *Object {
	return &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("this is a value in Riak"),
	}
}
