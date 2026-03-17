package ui

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

// ANSI color codes.
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// jsonMode tracks whether --json flag is set.
var jsonMode bool

// colorsEnabled tracks whether ANSI colors should be used.
var colorsEnabled bool

func init() {
	colorsEnabled = isTTY() && os.Getenv("NO_COLOR") == ""
}

// SetJSON sets the JSON output mode.
func SetJSON(enabled bool) {
	jsonMode = enabled
}

// IsJSON returns true if --json output mode is active.
func IsJSON() bool {
	return jsonMode
}

// isTTY returns true if stdout is a terminal.
func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// colorize wraps text in ANSI color codes if colors are enabled.
func colorize(color, msg string) string {
	if !colorsEnabled {
		return msg
	}
	return color + msg + colorReset
}

// Success prints a green success message.
func Success(msg string) {
	fmt.Println(colorize(colorGreen, "  OK    "+msg))
}

// Warn prints a yellow warning message.
func Warn(msg string) {
	fmt.Println(colorize(colorYellow, "  WARN  "+msg))
}

// Error prints a red error message.
func Error(msg string) {
	fmt.Println(colorize(colorRed, "  FAIL  "+msg))
}

// Info prints a cyan informational message.
func Info(msg string) {
	fmt.Println(colorize(colorCyan, "  INFO  "+msg))
}

// Bold prints bold text.
func Bold(msg string) string {
	return colorize(colorBold, msg)
}

// Table prints headers and rows as an aligned table using tabwriter.
func Table(headers []string, rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, strings.Join(headers, "\t"))
	sep := make([]string, len(headers))
	for i, h := range headers {
		sep[i] = strings.Repeat("-", len(h))
	}
	_, _ = fmt.Fprintln(w, strings.Join(sep, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush()
}

// JSON marshals v to indented JSON and prints it to stdout.
func JSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
