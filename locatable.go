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
	if l.BucketIsRequired() {
		if bucket := l.GetBucket(); len(bucket) == 0 {
			return ErrBucketRequired
		}
	}
	if l.KeyIsRequired() {
		if key := l.GetKey(); len(key) == 0 {
			return ErrKeyRequired
		}
	}
	if bucketType := l.GetType(); len(bucketType) == 0 {
		l.SetType([]byte(defaultBucketType))
	}
	return nil
}
