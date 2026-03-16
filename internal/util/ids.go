package util

import (
	"crypto/rand"
	"fmt"
)

// NewRunID generates a unique run ID.
func NewRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("run_%x", b)
}
