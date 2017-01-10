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
	"io"
	"log"
	"os"
)

// If true, debug messages will be written to the log
var EnableDebugLogging = false

var errLogger = log.New(os.Stderr, "", log.LstdFlags)
var logger = log.New(os.Stderr, "", log.LstdFlags)

func init() {
	if debugEnvVar := os.Getenv("RIAK_GO_CLIENT_DEBUG"); debugEnvVar != "" {
		EnableDebugLogging = true
	}
}

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

// logDebugln writes string debug messages using Println
func logDebugln(source string, v ...interface{}) {
	if EnableDebugLogging {
		logger.Println("[DEBUG]", source, v)
	}
}

// logWarn writes formatted string warning messages using Printf
func logWarn(source, format string, v ...interface{}) {
	logger.Printf(fmt.Sprintf("[WARNING] %s %s", source, format), v...)
}

// logWarnln writes string warning messages using Println
func logWarnln(source string, v ...interface{}) {
	logger.Println("[WARNING]", source, v)
}

// logError writes formatted string error messages using Printf
func logError(source, format string, v ...interface{}) {
	errLogger.Printf(fmt.Sprintf("[ERROR] %s %s", source, format), v...)
}

// logErr writes err.Error() using Println
func logErr(source string, err error) {
	errLogger.Println("[ERROR]", source, err)
}

// logErrorln writes an error message using Println
func logErrorln(source string, v ...interface{}) {
	errLogger.Println("[ERROR]", source, v)
}
