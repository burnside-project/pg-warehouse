package util

import "os"

// FileExists checks whether a file exists at the given path.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// EnsureDir creates a directory (and parents) if it doesn't exist.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}
