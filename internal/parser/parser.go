package parser

import (
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var (
	refRe    = regexp.MustCompile(`\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}`)
	sourceRe = regexp.MustCompile(`\{\{\s*source\(\s*'([^']+)'\s*(?:,\s*'([^']+)')?\s*\)\s*\}\}`)
	headerRe = regexp.MustCompile(`(?m)^--\s*(\w[\w_]*)\s*:\s*(.+?)\s*$`)
)

// ParseResult holds extracted metadata from a SQL model file.
type ParseResult struct {
	Refs            []string
	Sources         []SourceRef
	Header          map[string]string
	Materialization string
	Contract        string
	Tags            []string
	Name            string
}

// SourceRef represents a source('schema', 'table') reference.
type SourceRef struct {
	Schema string
	Table  string
}

// ParseFile reads and parses a SQL model file.
func ParseFile(path string) (*ParseResult, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read model file: %w", err)
	}
	return Parse(string(content))
}

// Parse extracts metadata from SQL content.
func Parse(content string) (*ParseResult, error) {
	result := &ParseResult{
		Header: make(map[string]string),
	}

	// Extract header metadata (-- key: value lines)
	for _, match := range headerRe.FindAllStringSubmatch(content, -1) {
		key := strings.ToLower(strings.TrimSpace(match[1]))
		value := strings.TrimSpace(match[2])
		result.Header[key] = value
	}

	// Set known header fields
	result.Name = result.Header["name"]
	result.Materialization = result.Header["materialized"]
	if result.Materialization == "" {
		result.Materialization = result.Header["materialization"]
	}
	if result.Materialization == "" {
		result.Materialization = "table"
	}
	result.Contract = result.Header["contract"]
	if tags, ok := result.Header["tags"]; ok {
		for _, t := range strings.Split(tags, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				result.Tags = append(result.Tags, t)
			}
		}
	}

	// Extract ref() dependencies
	for _, match := range refRe.FindAllStringSubmatch(content, -1) {
		result.Refs = append(result.Refs, match[1])
	}

	// Extract source() references
	for _, match := range sourceRe.FindAllStringSubmatch(content, -1) {
		ref := SourceRef{Schema: match[1]}
		if len(match) > 2 && match[2] != "" {
			ref.Table = match[2]
		}
		result.Sources = append(result.Sources, ref)
	}

	return result, nil
}

// Checksum computes SHA256 of file content.
func Checksum(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(content)), nil
}
