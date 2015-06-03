package riak

import (
	// "bytes"
	// "encoding/binary"
	"errors"
	"fmt"
	// "io"
	// "log"
	"net"
	// "syscall"
	"time"
	// "github.com/golang/protobuf/proto"
	// "github.com/basho-labs/riak-go-client/rpb"
)

const defaultMaxBuffer = 2048 * 1024
const defaultInitBuffer = 2 * 1024
const defaultRequestTimeout = time.Second * 4
const defaultConnectionTimeout = time.Second * 30

// TODO package-level variable ErrCannotRead is of type "error"
var (
	ErrOptionsRequired error = errors.New("options are required")
	ErrAddressRequired error = errors.New("RemoteAddress is required in options")
	ErrCannotRead      error = errors.New("cannot read from a non-active or closed connection")
	ErrCannotWrite     error = errors.New("cannot write to a non-active or closed connection")
)

type connectionOptions struct {
	remoteAddress     string
	connectionTimeout time.Duration
	requestTimeout    time.Duration
	maxBufferSize     uint
	initBufferSize    uint
	healthCheck       bool
}

// TODO authentication
type connection struct {
	addr              *net.TCPAddr
	conn              *net.TCPConn
	connectionTimeout time.Duration
	requestTimeout    time.Duration
	maxBufferSize     uint
	initBufferSize    uint
	healthCheck       bool
}

func newConnection(options *connectionOptions) (*connection, error) {
	if options == nil {
		return nil, ErrOptionsRequired
	}
	if options.remoteAddress == "" {
		return nil, ErrAddressRequired
	}
	resolvedAddress, err := net.ResolveTCPAddr("tcp", options.remoteAddress)
	if err != nil {
		return nil, fmt.Errorf("could not parse address %v|%v", options.remoteAddress, err)
	}
	if options.connectionTimeout == 0 {
		options.connectionTimeout = defaultConnectionTimeout
	}
	if options.requestTimeout == 0 {
		options.requestTimeout = defaultRequestTimeout
	}
	if options.maxBufferSize == 0 {
		options.maxBufferSize = defaultMaxBuffer
	}
	if options.initBufferSize == 0 {
		options.initBufferSize = defaultInitBuffer
	}
	return &connection{
		addr:              resolvedAddress,
		connectionTimeout: options.connectionTimeout,
		requestTimeout:    options.requestTimeout,
		maxBufferSize:     options.maxBufferSize,
		initBufferSize:    options.initBufferSize,
		healthCheck:       options.healthCheck,
	}, nil
}

/*
func connect (conn *connection) error {
}
*/
