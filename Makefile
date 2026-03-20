.PHONY: build run test lint fmt vet docker-build docker-up clean release-dry-run changelog \
       pipeline pipeline-silver pipeline-feat pipeline-preview pipeline-status \
       recipe-soak recipe-soak-preview recipe-soak-status

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

# ──────────────────────────────────────────────────────────────────────
# Pipeline (Medallion Architecture: raw → silver → feat)
# ──────────────────────────────────────────────────────────────────────

pipeline:
	@echo "Running full pipeline (silver → feat)..."
	./scripts/run-pipeline.sh

pipeline-silver:
	@echo "Running silver layer only..."
	./scripts/run-pipeline.sh --silver-only

pipeline-feat:
	@echo "Running feat layer only..."
	./scripts/run-pipeline.sh --feat-only

pipeline-preview:
	@echo "Previewing feat tables (10 rows each)..."
	./scripts/run-pipeline.sh --preview

pipeline-status:
	@echo "Recent pipeline runs:"
	@sqlite3 -header -column .pgwh/state.db \
		'SELECT * FROM feature_runs ORDER BY started_at DESC LIMIT 20;' 2>/dev/null \
		|| echo "No state database found. Run 'pg-warehouse init' first."

# ──────────────────────────────────────────────────────────────────────
# Recipe: Soak-Server (10.29.29.211 / soak_test)
# ──────────────────────────────────────────────────────────────────────

recipe-soak:
	@echo "Running soak-server recipe (silver → feat)..."
	./recipes/soak-server/run-pipeline.sh

recipe-soak-preview:
	@echo "Previewing soak-server feat tables (10 rows each)..."
	./recipes/soak-server/run-pipeline.sh --preview

recipe-soak-status:
	@echo "Recent soak-server pipeline runs:"
	@sqlite3 -header -column recipes/soak-server/.pgwh/state.db \
		'SELECT * FROM feature_runs ORDER BY started_at DESC LIMIT 20;' 2>/dev/null \
		|| echo "No state database found. Run 'pg-warehouse init --config recipes/soak-server/pg-warehouse.yml' first."
