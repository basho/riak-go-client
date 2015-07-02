package riak

import (
	"errors"
)

var ErrNilOptions error = errors.New("[Command] options must be non-nil")

type CommandBuilder interface {
	Build() (Command, error)
}

type Command interface {
	Name() string
	Success() bool
	rpbData() ([]byte, error)
	rpbRead(data []byte) error
}
