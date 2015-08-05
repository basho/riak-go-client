package riak

// Bare-bones logging to enable/disable debug logging

import (
	"io"
	"log"
	"os"
)

// If true, debug messages will be written to the log
var EnableDebugLogging = false

var errLogger = log.New(os.Stderr, "", log.LstdFlags)
var logger = log.New(os.Stderr, "", log.LstdFlags)

// setLogWriter replaces the default log writer, which uses Stderr
func setLogWriter(out io.Writer) {
	logger = log.New(out, "", log.LstdFlags)
}

// logDebug writes formatted string debug messages using Printf only if debug logging is enabled
func logDebug(format string, v ...interface{}) {
	if EnableDebugLogging {
		logger.Printf(format, v...)
	}
}

// logWarnln writes string warning messages using Pringln
func logWarnln(v string) {
	logger.Println(v)
}

// logError writes formatted string error messages using Printf
func logError(format string, v ...interface{}) {
	errLogger.Printf(format, v...)
}

// logErr writes err.Error() using Println
func logErr(err error) {
	errLogger.Println(err.Error())
}

// logErrorln writes a string error message using Println
func logErrorln(v string) {
	errLogger.Println(v)
}
