#!/usr/bin/env bash
set -euo pipefail

echo "Running tests with race detector..."
go test -race -count=1 -v ./...
echo "All tests passed."
