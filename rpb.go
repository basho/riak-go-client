package riak

import (
	"fmt"
)

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
		err = newClientError(fmt.Sprintf("expected response code %d, got: %d", expected, actual), nil)
	}
	return
}
