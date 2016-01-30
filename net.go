package riak

import (
	"net"
)

func isTemporaryNetError(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		return true
	} else {
		return false
	}
}
