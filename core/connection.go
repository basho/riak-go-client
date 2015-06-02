package core

import (
	// "bytes"
	// "encoding/binary"
	"errors"
	// "fmt"
	// "io"
	// "log"
	"net"
	// "syscall"

	// "github.com/golang/protobuf/proto"
	// "github.com/basho/riak-go-client/rpb"
)

// TODO package-level variable ErrCannotRead is of type "error"
var ErrCannotRead error = errors.New("cannot read from a non-active or closed connection")
var ErrCannotWrite error = errors.New("cannot write to a non-active or closed connection")

type Connection struct {
	addr    *net.TCPAddr       // address this node is associated with
	conn    *net.TCPConn  // connection
	active  bool          // whether connection is active or not
	// cluster chan *Session // access to the client's cluster channel
	debug   bool          // debugging info
}

func NewConnection(addr string) (*Connection, error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Connection{
		addr:    raddr,
		// cluster: cluster,
	}, nil
}

/*
// Dial attempts to connect to the Riak node.
func (s *Session) Dial() error {
	var err error
	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		return err
	}
	s.conn, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		if s.debug {
			log.Print(err.Error())
		}
		s.Close()
	} else {
		if s.debug {
			log.Printf("connected to: %s", s.addr)
		}
		s.conn.SetKeepAlive(true)
		s.active = true
	}
	return err
}

// check verifies the session is still connected and the Riak node can be accessed.
func (s *Session) check() {
	if s.debug {
		log.Printf("state for %s - active: %t", s.addr, s.active)
	}
	if !s.active {
		if s.debug {
			log.Printf("redialing: %s", s.addr)
		}
		if s.conn != nil {
			s.conn.Close()
		}
		if err := s.Dial(); err != nil {
			log.Print(err.Error())
		}
	}
	s.active = s.Ping()
}

func (s *Session) Available() bool {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Available: session paniced")
		}
	}()
	return (s.conn != nil && s.active)
}

func (s *Session) Release() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Release: session paniced")
		}
	}()
	s.cluster <- s
}

// Close the underlying net connection and set this session to inactive.
func (s *Session) Close() {
	s.active = false
	if s.conn != nil {
		s.conn.Close()
	}
}

// read response from the network connection.
func (s *Session) read() ([]byte, error) {
	if !s.Available() {
		return nil, ErrCannotRead
	}
	buf := make([]byte, 4)
	var size int32
	// first 4 bytes are always size of message
	if count, err := io.ReadFull(s.conn, buf); err == nil && count == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message and return it if no errors
		count, err := io.ReadFull(s.conn, data)
		if err != nil {
			if err == syscall.EPIPE {
				s.conn.Close()
			}
			s.active = false
			return nil, err
		}
		if count != int(size) {
			s.active = false
			return nil, errors.New(fmt.Sprintf("data length: %d, only read: %d", len(data), count))
		}
		return data, nil
	}
	return nil, nil
}

// write data to network connection.
func (s *Session) write(data []byte) error {
	if !s.Available() {
		return ErrCannotWrite
	}
	count, err := s.conn.Write(data)
	if err != nil {
		if err == syscall.EPIPE {
			s.conn.Close()
		}
		s.active = false
		return err
	}
	if count != len(data) {
		s.active = false
		return errors.New(fmt.Sprintf("data length: %d, only wrote: %d", len(data), count))
	}
	return nil
}

// execute does the full request/response cycle on a command using a single Node connection instance.
func (s *Session) execute(code byte, in []byte) (interface{}, error) {
	req, err := rpbWrite(code, in)
	if err != nil {
		return nil, err
	}

	if err := s.write(req); err != nil {
		return nil, err
	}

	resp, err := s.read()
	if err != nil {
		return nil, err
	}

	data, err := rpbRead(resp)
	if err != nil {
		// For some reason the connection isn't responding, set to inactive.
		// This could be an insufficient number of vnodes error, etc.
		if err == ErrZeroLength {
			s.active = false
		}
		return nil, err
	}
	return data, nil
}

// executeRead continues to read streaming value from the same connection.
func (s *Session) executeRead() (interface{}, error) {
	resp, err := s.read()
	if err != nil {
		return nil, err
	}

	data, err := rpbRead(resp)
	if err != nil {
		// For some reason the connection isn't responding, set to inactive.
		// This could be an insufficient number of vnodes error, etc.
		if err == ErrZeroLength {
			s.active = false
		}
		return nil, err
	}
	return data, nil
}

// GetBucket returns a new bucket to interact with on this session.
func (s *Session) GetBucket(name string) *Bucket {
	return &Bucket{
		session: s,
		name:    name,
		btype:   []byte("default"), // Riak automatically uses the 'default' namespace for buckets
	}
}

// Query returns a new query interface on this session.
func (s *Session) Query() *Query {
	return &Query{
		session: s,
	}
}

// ListBuckets returns a list of buckets from Riak.
//
// Riak Docs - Caution: This call can be expensive for the server - do not use in performance sensitive code.
func (s *Session) ListBuckets() ([]*Bucket, error) {
	out, err := s.execute(Messages["ListBucketsReq"], nil)
	if err != nil {
		return nil, err
	}
	blist := out.(*rpb.RpbListBucketsResp).GetBuckets()
	buckets := make([]*Bucket, len(blist))
	for i, name := range blist {
		buckets[i] = s.GetBucket(string(name))
	}
	return buckets, nil
}

// Ping is a server method which returns a Riak ping response.
//
// This method directly influences the state of the node attached to this session.
func (s *Session) Ping() bool {
	check, err := s.execute(Messages["PingReq"], nil)
	if err != nil {
		return false
	}
	return check.(bool)
}

// GetClientId gets the id set for this client.
func (s *Session) GetClientId() (*rpb.RpbGetClientIdResp, error) {
	out, err := s.execute(Messages["GetClientIdReq"], nil)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbGetClientIdResp), nil
}

// SetClientId sets the id for this client.
func (s *Session) SetClientId(id []byte) (bool, error) {
	opt := &rpb.RpbSetClientIdReq{
		ClientId: id,
	}
	in, err := proto.Marshal(opt)
	if err != nil {
		return false, err
	}
	out, err := s.execute(Messages["SetClientIdReq"], in)
	if err != nil {
		return false, err
	}
	return out.(bool), nil
}

// ServerInfo is a method which returns the information for the Riak cluster.
func (s *Session) ServerInfo() (*rpb.RpbGetServerInfoResp, error) {
	out, err := s.execute(Messages["GetServerInfoReq"], nil)
	if err != nil {
		return nil, err
	}
	return out.(*rpb.RpbGetServerInfoResp), nil
}
*/
