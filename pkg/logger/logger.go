package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Level represents logging severity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var levelNames = map[Level]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
	LevelFatal: "FATAL",
}

var levelFromString = map[string]Level{
	"debug": LevelDebug,
	"info":  LevelInfo,
	"warn":  LevelWarn,
	"error": LevelError,
	"fatal": LevelFatal,
}

// ParseLevel converts a string level name to its Level constant.
func ParseLevel(s string) Level {
	if l, ok := levelFromString[strings.ToLower(s)]; ok {
		return l
	}
	return LevelInfo
}

// Logger provides structured logging with configurable level, format, and output.
type Logger struct {
	mu     sync.Mutex
	level  Level
	output io.Writer
	json   bool
	fields map[string]interface{}
}

type logEntry struct {
	Timestamp string                 `json:"ts"`
	Level     string                 `json:"level"`
	Message   string                 `json:"msg"`
	Caller    string                 `json:"caller,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// New creates a Logger with the given level.
func New(level Level, jsonFormat bool) *Logger {
	return &Logger{
		level:  level,
		output: os.Stdout,
		json:   jsonFormat,
		fields: make(map[string]interface{}),
	}
}

// SetOutput changes the log output writer.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

// WithField returns a new Logger with the given key-value pair attached
// to every subsequent log line.
func (l *Logger) WithField(key string, value interface{}) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	clone := &Logger{
		level:  l.level,
		output: l.output,
		json:   l.json,
		fields: make(map[string]interface{}, len(l.fields)+1),
	}
	for k, v := range l.fields {
		clone.fields[k] = v
	}
	clone.fields[key] = value
	return clone
}

// WithFields returns a new Logger with additional key-value pairs.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	clone := &Logger{
		level:  l.level,
		output: l.output,
		json:   l.json,
		fields: make(map[string]interface{}, len(l.fields)+len(fields)),
	}
	for k, v := range l.fields {
		clone.fields[k] = v
	}
	for k, v := range fields {
		clone.fields[k] = v
	}
	return clone
}

func (l *Logger) log(lvl Level, msg string, args ...interface{}) {
	if lvl < l.level {
		return
	}

	formatted := msg
	if len(args) > 0 {
		formatted = fmt.Sprintf(msg, args...)
	}

	_, file, line, ok := runtime.Caller(2)
	caller := ""
	if ok {
		parts := strings.Split(file, "/")
		if len(parts) > 2 {
			parts = parts[len(parts)-2:]
		}
		caller = fmt.Sprintf("%s:%d", strings.Join(parts, "/"), line)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.json {
		entry := logEntry{
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     levelNames[lvl],
			Message:   formatted,
			Caller:    caller,
		}
		if len(l.fields) > 0 {
			entry.Fields = l.fields
		}
		data, err := json.Marshal(entry)
		if err != nil {
			fmt.Fprintf(l.output, "logger marshal error: %v\n", err)
			return
		}
		fmt.Fprintln(l.output, string(data))
	} else {
		ts := time.Now().Format("2006-01-02T15:04:05.000Z07:00")
		fieldStr := ""
		for k, v := range l.fields {
			fieldStr += fmt.Sprintf(" %s=%v", k, v)
		}
		fmt.Fprintf(l.output, "%s [%s] %s %s%s\n", ts, levelNames[lvl], caller, formatted, fieldStr)
	}

	if lvl == LevelFatal {
		os.Exit(1)
	}
}

// Debug writes a debug-level message.
func (l *Logger) Debug(msg string, args ...interface{}) {
	l.log(LevelDebug, msg, args...)
}

// Info writes an info-level message.
func (l *Logger) Info(msg string, args ...interface{}) {
	l.log(LevelInfo, msg, args...)
}

// Warn writes a warning-level message.
func (l *Logger) Warn(msg string, args ...interface{}) {
	l.log(LevelWarn, msg, args...)
}

// Error writes an error-level message.
func (l *Logger) Error(msg string, args ...interface{}) {
	l.log(LevelError, msg, args...)
}

// Fatal writes a fatal-level message and terminates the process.
func (l *Logger) Fatal(msg string, args ...interface{}) {
	l.log(LevelFatal, msg, args...)
}
