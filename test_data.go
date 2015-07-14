package riak

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
)

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

func integrationTestsBuildCluster() (*Cluster, error) {
	nodeOpts := &NodeOptions{
		RemoteAddress: remoteAddress,
	}

	var node *Node
	var err error
	if node, err = NewNode(nodeOpts); err != nil {
		return nil, err
	}
	if node == nil {
		return nil, errors.New("NewNode returned nil!")
	}

	nodes := []*Node{node}
	opts := &ClusterOptions{
		Nodes: nodes,
	}

	return NewCluster(opts)
}

func getBasicObject() *Object {
	return &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("this is a value in Riak"),
	}
}
