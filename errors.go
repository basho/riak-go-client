package riak

import errors "errors"

var (
	ErrOptionsRequired  error = errors.New("options are required")
	ErrAddressRequired  error = errors.New("RemoteAddress is required in options")
	ErrCannotRead       error = errors.New("cannot read from a non-active or closed connection")
	ErrCannotWrite      error = errors.New("cannot write to a non-active or closed connection")
	ErrBucketRequired   error = errors.New("bucket is required")
	ErrKeyRequired      error = errors.New("key is required")
	ErrExpectedResponse error = errors.New("expected a response from Riak but did not receive one")
)
