package riak

import errors "errors"

// Convenience variables used to generate errors throughout the library
var (
	ErrAddressRequired      = errors.New("RemoteAddress is required in options")
	ErrAuthMissingConfig    = errors.New("[Connection] authentication is missing TLS config")
	ErrAuthTLSUpgradeFailed = errors.New("[Connection] upgrading to TLS connection failed")
	ErrBucketRequired       = errors.New("Bucket is required")
	ErrCannotRead           = errors.New("Cannot read from a non-active or closed connection")
	ErrCannotWrite          = errors.New("Cannot write to a non-active or closed connection")
	ErrExpectedResponse     = errors.New("Expected a response from Riak but did not receive one")
	ErrKeyRequired          = errors.New("Key is required")
	ErrNilOptions           = errors.New("[Command] options must be non-nil")
	ErrOptionsRequired      = errors.New("Options are required")
)
