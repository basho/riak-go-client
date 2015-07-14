// +build integration

package riak

import "runtime"

func init() {
	runtime.GOMAXPROCS(2)
}
