// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

import errors "errors"

var (
	ErrOptionsRequired      error = errors.New("options are required")
	ErrAddressRequired      error = errors.New("RemoteAddress is required in options")
	ErrCannotRead           error = errors.New("cannot read from a non-active or closed connection")
	ErrCannotWrite          error = errors.New("cannot write to a non-active or closed connection")
	ErrBucketRequired       error = errors.New("bucket is required")
	ErrKeyRequired          error = errors.New("key is required")
	ErrExpectedResponse     error = errors.New("expected a response from Riak but did not receive one")
	ErrNilOptions           error = errors.New("[Command] options must be non-nil")
	ErrAuthMissingConfig    error = errors.New("[Connection] authentication is missing TLS config")
	ErrAuthTLSUpgradeFailed error = errors.New("[Connection] upgrading to TLS connection failed")
)
