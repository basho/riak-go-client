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
	onError(error)
	rpbData() ([]byte, error)
	rpbRead(data []byte) error
}

type CommandImpl struct {
	Error     error
	IsSuccess bool
}

func (cmd *CommandImpl) Success() bool {
	return cmd.IsSuccess == true
}

func (cmd *CommandImpl) onError(err error) {
	cmd.Error = err
	cmd.IsSuccess = false
}
