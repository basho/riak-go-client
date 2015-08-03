// Copyright 2015 Basho Technologies, Inc. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package riak

// Bare-bones logging to enable/disable debug logging

import (
	"io"
	"log"
	"os"
)

var EnableDebugLogging = false

var errLogger = log.New(os.Stderr, "", log.LstdFlags)
var logger = log.New(os.Stderr, "", log.LstdFlags)

func setLogWriter(out io.Writer) {
	logger = log.New(out, "", log.LstdFlags)
}

func logDebug(format string, v ...interface{}) {
	if EnableDebugLogging {
		logger.Printf(format, v...)
	}
}

func logWarnln(v string) {
	logger.Println(v)
}

func logError(format string, v ...interface{}) {
	errLogger.Printf(format, v...)
}

func logErr(err error) {
	errLogger.Println(err.Error())
}

func logErrorln(v string) {
	errLogger.Println(v)
}
