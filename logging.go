package riak

// Bare-bones logging to enable/disable debug logging

import (
	"io"
	"log"
	"os"
)

var EnableDebugLogging = true

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

func logError(format string, v ...interface{}) {
	errLogger.Printf(format, v...)
}

func logErrorln(v string) {
	errLogger.Println(v)
}
