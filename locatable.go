package riak

import (
	"fmt"
	proto "github.com/golang/protobuf/proto"
	"reflect"
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
	if l, ok := msg.(rpbLocatable); ok {
		if l.GetBucket() == nil && l.BucketIsRequired() {
			return ErrBucketRequired
		}
		if l.GetKey() == nil && l.KeyIsRequired() {
			return ErrKeyRequired
		}
		if l.GetType() == nil {
			l.SetType([]byte(defaultBucketType))
		}
	} else {
		return fmt.Errorf("could not cast %v into rpbLocatable", reflect.TypeOf(msg))
	}
	return nil
}
