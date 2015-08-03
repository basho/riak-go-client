// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

type CommandImpl struct {
	Error   error
	Success bool
}

func (cmd *CommandImpl) Successful() bool {
	return cmd.Success == true
}

func (cmd *CommandImpl) onError(err error) {
	cmd.Error = err
	cmd.Success = false
}
