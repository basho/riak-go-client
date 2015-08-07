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
