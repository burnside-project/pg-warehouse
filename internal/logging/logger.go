package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Level represents the logging level.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// Logger provides structured logging.
type Logger struct {
	level  Level
	format string
	logger *log.Logger
}

// NewLogger creates a new Logger with the specified level and format.
// Supported formats: "text" (default), "json".
func NewLogger(level string, format string) *Logger {
	if format == "" {
		format = "text"
	}
	return &Logger{
		level:  parseLevel(level),
		format: strings.ToLower(format),
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debug logs a debug-level message.
func (l *Logger) Debug(format string, args ...any) {
	if l.level <= LevelDebug {
		l.emit("DEBUG", format, args...)
	}
}

// Info logs an info-level message.
func (l *Logger) Info(format string, args ...any) {
	if l.level <= LevelInfo {
		l.emit("INFO", format, args...)
	}
}

// Warn logs a warning-level message.
func (l *Logger) Warn(format string, args ...any) {
	if l.level <= LevelWarn {
		l.emit("WARN", format, args...)
	}
}

// Error logs an error-level message.
func (l *Logger) Error(format string, args ...any) {
	if l.level <= LevelError {
		l.emit("ERROR", format, args...)
	}
}

// Fatal logs an error-level message and exits.
func (l *Logger) Fatal(format string, args ...any) {
	l.emit("FATAL", format, args...)
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func (l *Logger) emit(level string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if l.format == "json" {
		entry := map[string]string{
			"time":    time.Now().Format(time.RFC3339),
			"level":   level,
			"message": msg,
		}
		data, _ := json.Marshal(entry)
		fmt.Fprintln(os.Stderr, string(data))
	} else {
		l.logger.Printf("[%-5s] %s", level, msg)
	}
}

func parseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}
