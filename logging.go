package riak

// Bare-bones logging to enable/disable debug logging

import (
	"fmt"
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
func logDebug(source, format string, v ...interface{}) {
	if EnableDebugLogging {
		logger.Printf(fmt.Sprintf("[DEBUG] %s %s", source, format), v...)
	}
}

// logWarn writes formatted string warning messages using Printf
func logWarn(source, format string, v ...interface{}) {
	logger.Printf(fmt.Sprintf("[WARNING] %s %s", source, format), v...)
}

// logWarnln writes string warning messages using Println
func logWarnln(source, err string) {
	logger.Println("[WARNING]", source, err)
}

// logError writes formatted string error messages using Printf
func logError(source, format string, v ...interface{}) {
	errLogger.Printf(fmt.Sprintf("[ERROR] %s %s", source, format), v...)
}

// logErr writes err.Error() using Println
func logErr(source string, err error) {
	errLogger.Println("[ERROR]", source, err)
}

// logErrorln writes a string error message using Println
func logErrorln(source string, err string) {
	errLogger.Println("[ERROR]", source, err)
}
