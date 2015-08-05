package riak

import (
	proto "github.com/golang/protobuf/proto"
)

type rpbLocatable interface {
	GetType() []byte
	SetType(bt []byte) // NB: bt == bucket type
	BucketIsRequired() bool
	GetBucket() []byte
	KeyIsRequired() bool
	GetKey() []byte
}

func validateLocatable(msg proto.Message) error {
	l := msg.(rpbLocatable)
	if l.GetBucket() == nil && l.BucketIsRequired() {
		return ErrBucketRequired
	}
	if l.GetKey() == nil && l.KeyIsRequired() {
		return ErrKeyRequired
	}
	if l.GetType() == nil {
		l.SetType([]byte(defaultBucketType))
	}
	return nil
}
