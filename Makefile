.PHONY: build run test lint fmt vet docker-build docker-up clean release-dry-run changelog

BINARY := pg-warehouse
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags="-s -w -X github.com/burnside-project/pg-warehouse/pkg/version.Version=$(VERSION)"

build:
	CGO_ENABLED=1 go build $(LDFLAGS) -o $(BINARY) ./cmd/pg-warehouse/

run: build
	./$(BINARY) --help

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

docker-build:
	docker build -t pg-warehouse:$(VERSION) .

docker-up:
	docker compose up --build

clean:
	rm -f $(BINARY)
	rm -rf .pgwh/ *.duckdb out/

release-dry-run:
	goreleaser release --snapshot --skip=publish --clean

changelog:
	@echo "Unreleased changes since $$(git describe --tags --abbrev=0 2>/dev/null || echo 'initial commit'):"
	@echo ""
	@git log $$(git describe --tags --abbrev=0 2>/dev/null)..HEAD --oneline 2>/dev/null || git log --oneline
