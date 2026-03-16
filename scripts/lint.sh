#!/usr/bin/env bash
set -euo pipefail

echo "Running linters..."
golangci-lint run ./...
echo "Lint passed."
