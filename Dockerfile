# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w -X github.com/burnside-project/pg-warehouse/pkg/version.Version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o /pg-warehouse ./cmd/pg-warehouse/

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pg-warehouse /usr/local/bin/pg-warehouse

ENTRYPOINT ["pg-warehouse"]
