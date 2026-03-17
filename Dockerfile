# Build stage — using Debian for glibc compatibility with go-duckdb
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends git gcc g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w -X github.com/burnside-project/pg-warehouse/pkg/version.Version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o /pg-warehouse ./cmd/pg-warehouse/

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
COPY --from=builder /pg-warehouse /usr/local/bin/pg-warehouse

ENTRYPOINT ["pg-warehouse"]
