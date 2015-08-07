package riak

import errors "errors"

var (
	// ErrOptionsRequired is thrown when options are not included in a function call
	ErrOptionsRequired = errors.New("Options are required")

	// ErrAddressRequired is thrown when the RemoteAddress value is missing from the
	// connectionOptions object when passed into the newConnection function
	ErrAddressRequired = errors.New("RemoteAddress is required in options")

	// ErrCannotRead is thrown when
	ErrCannotRead = errors.New("Cannot read from a non-active or closed connection")

	// ErrCannotWrite is thrown when
	ErrCannotWrite = errors.New("Cannot write to a non-active or closed connection")

	// ErrBucketRequired is thrown when
	ErrBucketRequired = errors.New("Bucket is required")

	// ErrKeyRequired is thrown when
	ErrKeyRequired = errors.New("Key is required")

	// ErrExpectedResponse is thrown when
	ErrExpectedResponse = errors.New("Expected a response from Riak but did not receive one")

	// ErrNilOptions is thrown when
	ErrNilOptions = errors.New("[Command] options must be non-nil")

	// ErrAuthMissingConfig is thrown when
	ErrAuthMissingConfig = errors.New("[Connection] authentication is missing TLS config")

	// ErrAuthTLSUpgradeFailed is thrown when
	ErrAuthTLSUpgradeFailed = errors.New("[Connection] upgrading to TLS connection failed")
)
