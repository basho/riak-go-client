package riak

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
)

type fakeFancyLogger struct {
	buf        io.Writer
	lastCalled string
}

// appendNewline adds a newline, if one does not already exist at the
// end of the format string.  This mimics log.Print[f|ln]'s behavior.
func appendNewline(s string, w io.Writer) {
	// taken from stdlib's log.Printf
	if len(s) == 0 || s[len(s)-1] != '\n' {
		w.Write([]byte(string('\n')))
	}
}

func (logger *fakeFancyLogger) Println(v ...interface{}) {
	fmt.Fprintln(logger.buf, v...)
	logger.lastCalled = "Println"
}

func (logger *fakeFancyLogger) Printf(format string, v ...interface{}) {
	fmt.Fprintf(logger.buf, format, v...)
	appendNewline(format, logger.buf)
	logger.lastCalled = "Printf"
}

func (logger *fakeFancyLogger) Debug(v ...interface{}) {
	fmt.Fprintln(logger.buf, v...)
	logger.lastCalled = "Debug"
}

func (logger *fakeFancyLogger) Debugf(format string, v ...interface{}) {
	fmt.Fprintf(logger.buf, format, v...)
	appendNewline(format, logger.buf)
	logger.lastCalled = "Debugf"
}

func (logger *fakeFancyLogger) Warn(v ...interface{}) {
	fmt.Fprintln(logger.buf, v...)
	logger.lastCalled = "Warn"
}

func (logger *fakeFancyLogger) Warnf(format string, v ...interface{}) {
	fmt.Fprintf(logger.buf, format, v...)
	appendNewline(format, logger.buf)
	logger.lastCalled = "Warnf"
}

func (logger *fakeFancyLogger) Error(v ...interface{}) {
	fmt.Fprintln(logger.buf, v...)
	logger.lastCalled = "Error"
}

func (logger *fakeFancyLogger) Errorf(format string, v ...interface{}) {
	fmt.Fprintf(logger.buf, format, v...)
	appendNewline(format, logger.buf)
	logger.lastCalled = "Errorf"
}

func TestLog(t *testing.T) {
	EnableDebugLogging = true

	tests := []struct {
		setLoggerFunc func(Logger)
		logFunc       func(string, string, ...interface{})
		prefix        string
	}{
		{
			SetErrorLogger,
			logError,
			"[ERROR]",
		},
		{
			SetLogger,
			logWarn,
			"[WARNING]",
		},
		{
			SetLogger,
			logDebug,
			"[DEBUG]",
		},
	}

	for _, tt := range tests {
		buf := &bytes.Buffer{}
		logger := log.New(buf, "", log.LstdFlags)
		tt.setLoggerFunc(logger)
		tt.logFunc("[test]", "Hello %s!", "World")

		actual := buf.String()
		suffix := fmt.Sprintf("%s %s", tt.prefix, "[test] Hello World!\n")

		if !strings.HasSuffix(actual, suffix) {
			t.Errorf("Expected %s to end with %s", actual, suffix)
		}
	}
}

func TestFancyLog(t *testing.T) {
	EnableDebugLogging = true

	tests := []struct {
		setLoggerFunc func(Logger)
		logFunc       func(string, string, ...interface{})
		shouldCall    string
	}{
		{
			SetErrorLogger,
			logError,
			"Errorf",
		},
		{
			SetLogger,
			logWarn,
			"Warnf",
		},
		{
			SetLogger,
			logDebug,
			"Debugf",
		},
	}

	for _, tt := range tests {
		buf := &bytes.Buffer{}
		fancyLogger := &fakeFancyLogger{buf: buf}
		tt.setLoggerFunc(fancyLogger)
		tt.logFunc("[test]", "Hello %s!", "World")

		actual := buf.String()
		suffix := "[test] Hello World!\n"

		if !strings.HasSuffix(actual, suffix) {
			t.Errorf("Expected %s to end with %s", actual, suffix)
		}
		if fancyLogger.lastCalled != tt.shouldCall {
			t.Errorf("Expected call to %s, got %s", tt.shouldCall, fancyLogger.lastCalled)
		}
	}
}

func TestLogln(t *testing.T) {
	EnableDebugLogging = true

	tests := []struct {
		setLoggerFunc func(Logger)
		logFunc       func(string, ...interface{})
		prefix        string
	}{
		{
			SetErrorLogger,
			logErrorln,
			"[ERROR]",
		},
		{
			SetLogger,
			logWarnln,
			"[WARNING]",
		},
		{
			SetLogger,
			logDebugln,
			"[DEBUG]",
		},
	}

	for _, tt := range tests {
		buf := &bytes.Buffer{}
		logger := log.New(buf, "", log.LstdFlags)
		tt.setLoggerFunc(logger)
		tt.logFunc("[test]", "Hello", "World!")

		actual := buf.String()
		suffix := fmt.Sprintf("%s %s", tt.prefix, "[test] [Hello World!]\n")

		if !strings.HasSuffix(actual, suffix) {
			t.Errorf("Expected %s to end with %s", actual, suffix)
		}
	}
}

func TestFancyLogln(t *testing.T) {
	EnableDebugLogging = true

	tests := []struct {
		setLoggerFunc func(Logger)
		logFunc       func(string, ...interface{})
		shouldCall    string
	}{
		{
			SetErrorLogger,
			logErrorln,
			"Error",
		},
		{
			SetLogger,
			logWarnln,
			"Warn",
		},
		{
			SetLogger,
			logDebugln,
			"Debug",
		},
	}

	for _, tt := range tests {
		buf := &bytes.Buffer{}
		fancyLogger := &fakeFancyLogger{buf: buf}
		tt.setLoggerFunc(fancyLogger)
		tt.logFunc("[test]", "Hello", "World!")

		actual := buf.String()
		suffix := "[test] [Hello World!]\n"

		if !strings.HasSuffix(actual, suffix) {
			t.Errorf("Expected %s to end with %s", actual, suffix)
		}
		if fancyLogger.lastCalled != tt.shouldCall {
			t.Errorf("Expected call to %s, got %s", tt.shouldCall, fancyLogger.lastCalled)
		}
	}
}

func TestDebugDisabled(t *testing.T) {
	EnableDebugLogging = false

	buf := &bytes.Buffer{}
	logger := log.New(buf, "", log.LstdFlags)
	SetLogger(logger)

	logDebug("[test]", "Hello %s!", "World")
	logDebugln("[test]", "Hello", "World")

	actual := buf.String()

	if len(actual) != 0 {
		t.Errorf("Debug was disabled but got %s", actual)
	}
}
