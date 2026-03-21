# Architecture

pg-warehouse uses **Hexagonal Architecture** (ports and adapters) to keep domain logic independent from infrastructure.

## Layer Diagram

```
CLI (Cobra commands)
 ↓
Application Services (orchestration)
 ↓
Ports (interface contracts)
 ↓
Adapters (concrete implementations)
 ↓
Infrastructure (PostgreSQL, DuckDB, SQLite)
```

## Data Flow

```
PostgreSQL (source)
   ↓ WAL streaming (CDC, never stops)

raw.duckdb (CDC BLACK BOX)
   ├── stage.*    CDC append buffer (_epoch, _deleted)
   │     ↓ merge (dedup by PK, every 60s)
   └── raw.*      deduped source tables (one row per PK, clean)
         ↓
         ↓ pg-warehouse run --refresh (snapshot + full copy)
         ↓
silver.duckdb (DEVELOPMENT PLATFORM)
   ├── v0.*       copy of raw.* (read-only, frozen between refreshes)
   │     ↓ pg-warehouse run --pipeline (001.sql, 002.sql... in order)
   ├── v1.*       silver transforms (your SQL)
   │     ↓ pg-warehouse run --promote (swap views)
   ├── current.*  views → active version
   └── _meta.*    versions, refresh log
         ↓
         ↓ pg-warehouse run --refresh (copy current.* to feature v0)
         ↓
feature.duckdb (ANALYTICS OUTPUT)
   ├── v0.*       copy of silver current.* (read-only)
   │     ↓ pg-warehouse run --pipeline (001.sql, 002.sql... in order)
   ├── v1.*       feature transforms (your SQL)
   │     ↓ pg-warehouse run --promote (swap views)
   ├── current.*  views → active version
   └── _meta.*    versions, refresh log
         ↓
   out/*.parquet → Dashboard → AI Q&A
```

See [Multi-DuckDB Architecture](09-multi-duckdb-architecture.md) for the full design.

## State Flow

```
SQLite (.pgwh/state.db)
   ├── sync_state       — per-table sync watermarks and LSN
   ├── sync_history     — bounded sync run history
   ├── cdc_state        — per-table CDC replication state
   ├── feature_runs     — bounded feature execution history
   ├── audit_log        — bounded platform audit trail
   ├── watermarks       — named progress checkpoints
   ├── lock_state       — concurrent execution prevention
   ├── project_identity — project metadata
   └── schema_version   — migration tracking
```

## Port Interfaces

| Port | Purpose | Adapter(s) |
|------|---------|------------|
| `PostgresSource` | Read data from PostgreSQL | `postgres.Source` (pgx/v5 pool) |
| `WarehouseStore` | DuckDB warehouse operations | `duckdb.Warehouse` |
| `MetadataStore` | Sync/feature metadata CRUD | `sqlitestate.Store` |
| `StateStore` | Extended state management (embeds MetadataStore) | `sqlitestate.Store` |
| `CDCSource` | PostgreSQL logical replication | `postgres.CDCAdapter` (pglogrepl) |
| `Inspector` | Warehouse introspection | `duckdb.Inspector` |
| `ConfigLoader` | YAML config loading | `fileconfig.Loader` |
| `Exporter` | File format export | `parquet.Exporter`, `csv.Exporter` |

## Services

| Service | Responsibility |
|---------|---------------|
| `InitService` | Project initialization, DuckDB bootstrap, PG connectivity check |
| `SyncService` | Full snapshot and watermark-based incremental sync |
| `CDCService` | CDC lifecycle: setup, snapshot, epoch management, streaming, teardown |
| `RunService` | SQL feature file execution with target validation and export |
| `SilverService` | Silver version management: create, promote, compare, drop (multi-file mode) |
| `FeatureService` | Feature pipeline execution with ATTACH isolation (multi-file mode) |
| `PreviewService` | SQL preview with sample rows |
| `InspectService` | Warehouse table listing, schema description, sync state |
| `ExportService` | Table export to Parquet/CSV with directory creation |
| `DoctorService` | Configuration and connectivity validation |

## Key Design Rules

1. **Ports define contracts.** Adapters implement those contracts.
2. **Adapters must not call each other directly.** All coordination goes through services.
3. **Domain contains pure business logic.** No infrastructure imports.
4. **State is decoupled from data.** SQLite state DB survives DuckDB rebuilds.
5. **raw.duckdb is a black box.** CDC owns it exclusively. Users read raw.* only via `--refresh`.
6. **Silver reads from raw only. Feat reads from silver only.** Strict layer isolation.
7. **Epochs guarantee consistency.** Pipeline only reads merged (committed) epochs.

## Directory Structure

```
pg-warehouse/
├── cmd/pg-warehouse/     # CLI commands (Cobra)
│   ├── main.go, root.go  # Entry point
│   ├── app.go            # Shared component wiring
│   ├── init.go, sync.go  # Data commands
│   ├── cdc.go            # CDC commands
│   ├── run.go, preview.go, export.go
│   ├── inspect.go        # Inspection commands
│   └── doctor.go         # Diagnostics
├── internal/
│   ├── domain/           # Pure business logic
│   │   ├── sync/         # Sync mode rules, state types
│   │   ├── feature/      # Feature validation rules
│   │   └── warehouse/    # Schema naming conventions
│   ├── ports/            # Interface contracts
│   ├── adapters/         # Infrastructure implementations
│   │   ├── postgres/     # PG source + CDC (pgx/v5, pglogrepl)
│   │   ├── duckdb/       # Warehouse + inspector
│   │   ├── sqlitestate/  # SQLite state DB
│   │   ├── parquet/      # Parquet exporter
│   │   ├── csv/          # CSV exporter
│   │   ├── fileconfig/   # YAML config loader
│   │   └── localmeta/    # (deprecated) DuckDB metadata
│   ├── services/         # Application services
│   ├── models/           # Data structures
│   ├── config/           # Config defaults and validation
│   ├── logging/          # Logger
│   └── util/             # Utilities (IDs, files)
├── sql/bootstrap/        # Bootstrap SQL
├── examples/             # Example config and SQL
├── test/                 # Integration tests
└── docs/                 # Documentation
```
