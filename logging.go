package riak

// Bare-bones logging to enable/disable debug logging

import (
	"io"
	"log"
	"os"
)

var EnableDebugLogging = true

var logger = log.New(os.Stderr, "", log.LstdFlags)

func setLogWriter(out io.Writer) {
	logger = log.New(out, "", log.LstdFlags)
}

func debug(format string, v ...interface{}) {
	if EnableDebugLogging {
		logger.Printf(format, v...)
	}
}
