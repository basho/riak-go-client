package riak

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

const defaultRequestTimeout = time.Second * 4
const defaultConnectionTimeout = time.Second * 30

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
	healthCheck       bool
}

// TODO authentication
type connection struct {
	addr              *net.TCPAddr
	connection        net.Conn
	connectionTimeout time.Duration
	requestTimeout    time.Duration
	healthCheck       bool
	active            bool
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
	return &connection{
		addr:              resolvedAddress,
		connectionTimeout: options.connectionTimeout,
		requestTimeout:    options.requestTimeout,
		healthCheck:       options.healthCheck,
	}, nil
}

func (c *connection) connect() (err error) {
	dialer := &net.Dialer{
		Timeout:   c.connectionTimeout,
		KeepAlive: time.Second * 30,
	}
	c.connection, err = dialer.Dial("tcp", c.addr.String())
	if err != nil {
		logError(err.Error())
		c.close()
	} else {
		logDebug("connected to: %s", c.addr)
		c.active = true
	}
	return
}

func (c *connection) available() bool {
	defer func() {
		if err := recover(); err != nil {
			logErrorln("available: connection panic!")
		}
	}()
	return (c.connection != nil && c.active)
}

func (c *connection) close() error {
	return c.connection.Close()
}

func (c *connection) execute(cmd Command) (err error) {
	if err = c.write(cmd.rpbData()); err != nil {
		return
	}

	data, err := c.read()
	if err != nil {
		return
	}

	cmd.rpbRead(data)
	return
}

func (c *connection) read() ([]byte, error) {
	if !c.available() {
		return nil, ErrCannotRead
	}
	buf := make([]byte, 4)
	if count, err := io.ReadFull(c.connection, buf); err == nil && count == 4 {
		size := binary.BigEndian.Uint32(buf)
		data := make([]byte, size)
		count, err := io.ReadFull(c.connection, data)
		if err != nil {
			if err == syscall.EPIPE {
				c.close()
			}
			c.active = false
			return nil, err
		}
		if count != int(size) {
			c.active = false
			return nil, errors.New(fmt.Sprintf("data length: %d, only read: %d", len(data), count))
		}
		return data, nil
	}
	return nil, nil
}

func (c *connection) write(data []byte) error {
	if !c.available() {
		return ErrCannotWrite
	}
	count, err := c.connection.Write(data)
	if err != nil {
		if err == syscall.EPIPE {
			c.close()
		}
		c.active = false
		return err
	}
	if count != len(data) {
		c.active = false
		return errors.New(fmt.Sprintf("data length: %d, only wrote: %d", len(data), count))
	}
	return nil
}
