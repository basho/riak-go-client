// Copyright 2015-present Basho Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package riak

// Bare-bones logging to enable/disable debug logging

import (
	"fmt"
	"log"
	"os"
)

// Logger is a generic logging interface, to allow utilizing
// any stdlib-compatible logger for logging
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// FancyLogger represents a leveled logger, with methods representing
// debug, warn, and error levels
type FancyLogger interface {
	Logger
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
}

// If true, debug messages will be written to the log
var EnableDebugLogging = false

var errLogger Logger
var stdLogger Logger

func init() {
	if debugEnvVar := os.Getenv("RIAK_GO_CLIENT_DEBUG"); debugEnvVar != "" {
		EnableDebugLogging = true
	}
	errLogger = log.New(os.Stderr, "", log.LstdFlags)
	stdLogger = log.New(os.Stderr, "", log.LstdFlags)
}

// SetLogger sets the standard logger used for
// WARN and DEBUG (if enabled)
func SetLogger(logger Logger) {
	stdLogger = logger
}

// SetErrorLogger sets the logger used for errors
func SetErrorLogger(logger Logger) {
	errLogger = logger
}

// logDebug writes formatted string debug messages using Printf only if debug logging is enabled
func logDebug(source, format string, v ...interface{}) {
	if EnableDebugLogging {
		if fancyLogger, ok := stdLogger.(FancyLogger); ok {
			fancyLogger.Debugf(fmt.Sprintf("%s %s", source, format), v...)
		} else {
			stdLogger.Printf(fmt.Sprintf("[DEBUG] %s %s", source, format), v...)
		}
	}
}

// logDebugln writes string debug messages using Println
func logDebugln(source string, v ...interface{}) {
	if EnableDebugLogging {
		if fancyLogger, ok := stdLogger.(FancyLogger); ok {
			fancyLogger.Debug(source, v)
		} else {
			stdLogger.Println("[DEBUG]", source, v)
		}
	}
}

// logWarn writes formatted string warning messages using Printf
func logWarn(source, format string, v ...interface{}) {
	if fancyLogger, ok := stdLogger.(FancyLogger); ok {
		fancyLogger.Warnf(fmt.Sprintf("%s %s", source, format), v...)
	} else {
		stdLogger.Printf(fmt.Sprintf("[WARNING] %s %s", source, format), v...)
	}
}

// logWarnln writes string warning messages using Println
func logWarnln(source string, v ...interface{}) {
	if fancyLogger, ok := stdLogger.(FancyLogger); ok {
		fancyLogger.Warn(source, v)
	} else {
		stdLogger.Println("[WARNING]", source, v)
	}
}

// logError writes formatted string error messages using Printf
func logError(source, format string, v ...interface{}) {
	if fancyLogger, ok := errLogger.(FancyLogger); ok {
		fancyLogger.Errorf(fmt.Sprintf("%s %s", source, format), v...)
	} else {
		errLogger.Printf(fmt.Sprintf("[ERROR] %s %s", source, format), v...)
	}
}

// logErr writes err.Error() using Println
func logErr(source string, err error) {
	if fancyLogger, ok := errLogger.(FancyLogger); ok {
		fancyLogger.Error(source, err)
	} else {
		errLogger.Println("[ERROR]", source, err)
	}
}

// logErrorln writes an error message using Println
func logErrorln(source string, v ...interface{}) {
	if fancyLogger, ok := errLogger.(FancyLogger); ok {
		fancyLogger.Error(source, v)
	} else {
		errLogger.Println("[ERROR]", source, v)
	}
}
