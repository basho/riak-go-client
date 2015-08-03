// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

import (
	"errors"
	"fmt"
)

var ErrZeroLength error = errors.New("[Command] 0 byte data response")

func rpbValidateResp(data []byte, expected byte) (err error) {
	if len(data) == 0 {
		err = ErrZeroLength
		return
	}
	if err = rpbEnsureCode(expected, data[0]); err != nil {
		return
	}
	return
}

func rpbEnsureCode(expected byte, actual byte) (err error) {
	if expected != actual {
		err = fmt.Errorf("expected response code %d, got: %d", expected, actual)
	}
	return
}
