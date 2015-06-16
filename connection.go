package riak

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

type connectionOptions struct {
	remoteAddress  *net.TCPAddr
	connectTimeout time.Duration
	requestTimeout time.Duration
	healthCheck    Command
}

// TODO authentication
type connection struct {
	addr           *net.TCPAddr
	conn           net.Conn
	connectTimeout time.Duration
	requestTimeout time.Duration
	healthCheck    Command
	sizeBuf        []byte
	active         bool
	inFlight       bool
	lastUsed       time.Time
}

func newConnection(options *connectionOptions) (*connection, error) {
	if options == nil {
		return nil, ErrOptionsRequired
	}
	if options.remoteAddress == nil {
		return nil, ErrAddressRequired
	}
	if options.connectTimeout == 0 {
		options.connectTimeout = defaultConnectTimeout
	}
	if options.requestTimeout == 0 {
		options.requestTimeout = defaultRequestTimeout
	}
	return &connection{
		addr:           options.remoteAddress,
		connectTimeout: options.connectTimeout,
		requestTimeout: options.requestTimeout,
		healthCheck:    options.healthCheck,
		sizeBuf:        make([]byte, 4),
		active:         false,
		inFlight:       false,
		lastUsed:       time.Now(),
	}, nil
}

func (c *connection) connect() (err error) {
	dialer := &net.Dialer{
		Timeout:   c.connectTimeout,
		KeepAlive: time.Second * 30,
	}
	c.conn, err = dialer.Dial("tcp", c.addr.String())
	if err != nil {
		logError(err.Error())
		c.close()
	} else {
		logDebug("connected to: %s", c.addr)
		c.active = true
		if c.healthCheck != nil {
			if err = c.execute(c.healthCheck); err != nil || !c.healthCheck.Success() {
				c.active = false
				logError(err.Error())
				c.close()
			}
		}
	}
	return
}

func (c *connection) available() bool {
	defer func() {
		if err := recover(); err != nil {
			logErrorln("[Connection] available: connection panic!")
		}
	}()
	return (c.conn != nil && c.active)
}

func (c *connection) close() error {
	return c.conn.Close()
}

func (c *connection) execute(cmd Command) (err error) {
	if c.inFlight == true {
		err = fmt.Errorf("[Connection] attempted to run command on in-use connection")
		return
	}

	logDebug("[Connection] execute command: %v", cmd.Name())
	c.inFlight = true
	c.lastUsed = time.Now()

	if err = c.write(cmd.rpbData()); err != nil {
		return
	}

	data, err := c.read()
	if err != nil {
		return
	}

	if err = cmd.rpbRead(data); err != nil {
		return
	}

	// TODO streaming responses
	// TODO translate RpbErrorResp into golang error
	return
}

// TODO: we should also take currently executing Command (Riak operation)
// timeout into account
func (c *connection) setReadDeadline() {
	c.conn.SetReadDeadline(time.Now().Add(c.requestTimeout))
}

func (c *connection) read() ([]byte, error) {
	if !c.available() {
		return nil, ErrCannotRead
	}
	c.setReadDeadline()
	if count, err := io.ReadFull(c.conn, c.sizeBuf); err == nil && count == 4 {
		size := binary.BigEndian.Uint32(c.sizeBuf)
		// TODO: investigate using a buffer on c instead of
		// always making a new one
		data := make([]byte, size)
		c.setReadDeadline()
		count, err := io.ReadFull(c.conn, data)
		if err != nil {
			if err == syscall.EPIPE {
				c.close()
			}
			c.active = false
			return nil, err
		}
		if count != int(size) {
			c.active = false
			return nil, fmt.Errorf("data length: %d, only read: %d", len(data), count)
		}
		return data, nil
	}
	return nil, nil
}

func (c *connection) write(data []byte) error {
	if !c.available() {
		return ErrCannotWrite
	}
	// TODO: we should also take currently executing Command (Riak operation)
	// timeout into account
	c.conn.SetWriteDeadline(time.Now().Add(c.requestTimeout))
	count, err := c.conn.Write(data)
	if err != nil {
		if err == syscall.EPIPE {
			c.close()
		}
		c.active = false
		return err
	}
	if count != len(data) {
		c.active = false
		return fmt.Errorf("data length: %d, only wrote: %d", len(data), count)
	}
	return nil
}
