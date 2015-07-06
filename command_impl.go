package riak

import (
	"errors"
)

var ErrNilOptions error = errors.New("[Command] options must be non-nil")

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
