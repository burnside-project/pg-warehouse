package logging

import (
	"fmt"
	"log"
	"os"
	"strings"
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
	logger *log.Logger
}

// NewLogger creates a new Logger.
func NewLogger(level string) *Logger {
	return &Logger{
		level:  parseLevel(level),
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debug logs a debug-level message.
func (l *Logger) Debug(format string, args ...any) {
	if l.level <= LevelDebug {
		l.logger.Printf("[DEBUG] "+format, args...)
	}
}

// Info logs an info-level message.
func (l *Logger) Info(format string, args ...any) {
	if l.level <= LevelInfo {
		l.logger.Printf("[INFO]  "+format, args...)
	}
}

// Warn logs a warning-level message.
func (l *Logger) Warn(format string, args ...any) {
	if l.level <= LevelWarn {
		l.logger.Printf("[WARN]  "+format, args...)
	}
}

// Error logs an error-level message.
func (l *Logger) Error(format string, args ...any) {
	if l.level <= LevelError {
		l.logger.Printf("[ERROR] "+format, args...)
	}
}

// Fatal logs an error-level message and exits.
func (l *Logger) Fatal(format string, args ...any) {
	l.logger.Printf("[FATAL] "+format, args...)
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
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
