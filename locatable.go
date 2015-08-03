// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

import (
	"fmt"
	"reflect"

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
