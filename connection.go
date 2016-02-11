package riak

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	proto "github.com/golang/protobuf/proto"
)

// Connection errors
var (
	ErrCannotRead  = errors.New("Cannot read from a non-active or closed connection")
	ErrCannotWrite = errors.New("Cannot write to a non-active or closed connection")
)

// AuthOptions object contains the authentication credentials and tls config
type AuthOptions struct {
	User      string
	Password  string
	TlsConfig *tls.Config
}

type connectionOptions struct {
	remoteAddress  *net.TCPAddr
	connectTimeout time.Duration
	requestTimeout time.Duration
	authOptions    *AuthOptions
}

const (
	connCreated state = iota
	connTlsStarting
	connActive
	connInactive
)

type connection struct {
	addr           *net.TCPAddr
	conn           net.Conn
	connectTimeout time.Duration
	requestTimeout time.Duration
	authOptions    *AuthOptions
	sizeBuf        []byte
	dataBuf        []byte
	active         bool
	inFlight       bool
	lastUsed       time.Time
	stateData
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
	c := &connection{
		addr:           options.remoteAddress,
		connectTimeout: options.connectTimeout,
		requestTimeout: options.requestTimeout,
		authOptions:    options.authOptions,
		sizeBuf:        make([]byte, 4),
		dataBuf:        make([]byte, defaultInitBuffer),
		inFlight:       false,
		lastUsed:       time.Now(),
	}
	c.initStateData("connCreated", "connTlsStarting", "connActive", "connInactive")
	c.setState(connCreated)
	return c, nil
}

func (c *connection) connect() (err error) {
	dialer := &net.Dialer{
		Timeout:   c.connectTimeout,
		KeepAlive: time.Second * 30,
	}
	c.conn, err = dialer.Dial("tcp", c.addr.String()) // NB: SetNoDelay() is true by default for TCP connections
	if err != nil {
		logError("[Connection]", "error when dialing %s: '%s'", c.addr.String(), err.Error())
		c.close()
	} else {
		logDebug("[Connection]", "connected to: %s", c.addr)
		if err = c.startTls(); err != nil {
			c.close()
			c.setState(connInactive)
			return
		}
		c.setState(connActive)
	}
	return
}

func (c *connection) startTls() error {
	if c.authOptions == nil {
		return nil
	}
	if c.authOptions.TlsConfig == nil {
		return ErrAuthMissingConfig
	}
	c.setState(connTlsStarting)
	startTlsCmd := &startTlsCommand{}
	if err := c.execute(startTlsCmd); err != nil {
		return err
	}
	var tlsConn *tls.Conn
	if tlsConn = tls.Client(c.conn, c.authOptions.TlsConfig); tlsConn == nil {
		return ErrAuthTLSUpgradeFailed
	}
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	c.conn = tlsConn
	authCmd := &authCommand{
		user:     c.authOptions.User,
		password: c.authOptions.Password,
	}
	return c.execute(authCmd)
}

func (c *connection) available() bool {
	return (c.conn != nil && c.isStateLessThan(connInactive))
}

func (c *connection) close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *connection) setInFlight(inFlightVal bool) {
	c.inFlight = inFlightVal
}

func (c *connection) execute(cmd Command) (err error) {
	if c.inFlight == true {
		err = fmt.Errorf("[Connection] attempted to run '%s' command on in-use connection", cmd.Name())
		return
	}

	c.setInFlight(true)
	defer c.setInFlight(false)
	c.lastUsed = time.Now()

	var message []byte
	message, err = getRiakMessage(cmd)
	if err != nil {
		return
	}

	if err = c.write(message); err != nil {
		return
	}

	var response []byte
	var decoded proto.Message
	for {
		response, err = c.read() // NB: response *will* have entire pb message
		if err != nil {
			cmd.onError(err)
			return
		}

		// Maybe translate RpbErrorResp into golang error
		if err = maybeRiakError(response); err != nil {
			cmd.onError(err)
			return
		}

		if decoded, err = decodeRiakMessage(cmd, response); err != nil {
			cmd.onError(err)
			return
		}

		err = cmd.onSuccess(decoded)
		if err != nil {
			cmd.onError(err)
			return
		}

		if sc, ok := cmd.(streamingCommand); ok {
			// Streaming Commands indicate done
			if sc.isDone() {
				return
			}
		} else {
			// non-streaming command, done at this point
			return
		}
	}
}

// FUTURE: we should also take currently executing Command (Riak operation)
// timeout into account
func (c *connection) setReadDeadline() {
	c.conn.SetReadDeadline(time.Now().Add(c.requestTimeout))
}

// NB: This will read one full pb message from Riak, or error in doing so
func (c *connection) read() ([]byte, error) {
	if !c.available() {
		return nil, ErrCannotRead
	}

	var err error
	var count int
	var messageLength uint32

	c.setReadDeadline()
	if count, err = io.ReadFull(c.conn, c.sizeBuf); err == nil && count == 4 {
		messageLength = binary.BigEndian.Uint32(c.sizeBuf)
		if messageLength > uint32(cap(c.dataBuf)) {
			logDebug("[Connection]", "allocating larger dataBuf of size %d", messageLength)
			c.dataBuf = make([]byte, messageLength)
		} else {
			c.dataBuf = c.dataBuf[0:messageLength]
		}
		// FUTURE: large object warning / error
		c.setReadDeadline()
		count, err = io.ReadFull(c.conn, c.dataBuf)
	} else {
		if err == nil && count != 4 {
			err = newClientError(fmt.Sprintf("[Connection] expected to read 4 bytes, only read: %d", count), nil)
		}
	}

	if err == nil && count != int(messageLength) {
		err = newClientError(fmt.Sprintf("[Connection] message length: %d, only read: %d", messageLength, count), nil)
	}

	if err == nil {
		return c.dataBuf, nil
	} else {
		if !isTemporaryNetError(err) {
			c.setState(connInactive)
		}
		return nil, err
	}
}

func (c *connection) write(data []byte) error {
	if !c.available() {
		return ErrCannotWrite
	}
	// FUTURE: we should also take currently executing Command (Riak operation) timeout into account
	c.conn.SetWriteDeadline(time.Now().Add(c.requestTimeout))
	count, err := c.conn.Write(data)
	if err != nil {
		logDebug("[Connection]", "error in write: '%v'", err)
		if !isTemporaryNetError(err) {
			c.setState(connInactive)
		}
		return err
	}
	if count != len(data) {
		return newClientError(fmt.Sprintf("[Connection] data length: %d, only wrote: %d", len(data), count), nil)
	}
	return nil
}
