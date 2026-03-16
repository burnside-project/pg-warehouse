#!/usr/bin/env bash
set -euo pipefail

echo "Installing development dependencies..."

# golangci-lint
if ! command -v golangci-lint &>/dev/null; then
    echo "Installing golangci-lint..."
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
fi

echo "Running go mod tidy..."
go mod tidy

echo "Development environment ready."
