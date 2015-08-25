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
	healthCheck    Command
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
	healthCheck    Command
	authOptions    *AuthOptions
	sizeBuf        []byte
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
		healthCheck:    options.healthCheck,
		authOptions:    options.authOptions,
		sizeBuf:        make([]byte, 4),
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
			c.setState(connInactive)
			return
		}
		c.setState(connActive)
		if c.healthCheck != nil {
			if err = c.execute(c.healthCheck); err != nil || !c.healthCheck.Success() {
				c.setState(connInactive)
				logError("[Connection]", "initial health check error: '%s'", err.Error())
				c.close()
			}
		}
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
	startTlsCmd := &StartTlsCommand{}
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
	authCmd := &AuthCommand{
		User:     c.authOptions.User,
		Password: c.authOptions.Password,
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

	logDebug("[Connection]", "execute command: %v", cmd.Name())
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

		if sc, ok := cmd.(StreamingCommand); ok {
			// Streaming Commands indicate done
			if sc.Done() {
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
	var data []byte
	c.setReadDeadline()
	if count, err = io.ReadFull(c.conn, c.sizeBuf); err == nil && count == 4 {
		messageLength := binary.BigEndian.Uint32(c.sizeBuf)
		// TODO: investigate using a bytes.Buffer on c instead of
		// always making a new byte slice, more in-line with Node.js client
		data = make([]byte, messageLength)
		c.setReadDeadline()
		count, err = io.ReadFull(c.conn, data)
		if err == nil && uint32(count) != messageLength {
			err = newClientError(fmt.Sprintf("[Connection] message length: %d, only read: %d", messageLength, count))
		}
	}
	if err != nil {
		logDebug("[Connection]", "error in read: '%v'", err)
		// connection will eventually be expired
		c.setState(connInactive)
		data = nil
	}
	return data, err
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
		c.setState(connInactive)
		return err
	}
	if count != len(data) {
		// connection will eventually be expired
		c.setState(connInactive)
		return newClientError(fmt.Sprintf("[Connection] data length: %d, only wrote: %d", len(data), count))
	}
	return nil
}
