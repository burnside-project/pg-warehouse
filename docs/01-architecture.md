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
   ↓ snapshot / watermark / CDC
DuckDB (local warehouse)
   ├── raw.*   — mirrored source tables
   ├── stage.* — temporary staging for incremental merge
   └── feat.*  — SQL feature pipeline outputs
   ↓
Parquet / CSV exports
```

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
| `CDCService` | CDC lifecycle: setup, snapshot, streaming, teardown |
| `RunService` | SQL feature file execution with target validation and export |
| `PreviewService` | SQL preview with sample rows |
| `InspectService` | Warehouse table listing, schema description, sync state |
| `ExportService` | Table export to Parquet/CSV with directory creation |
| `DoctorService` | Configuration and connectivity validation |

## Key Design Rules

1. **Ports define contracts.** Adapters implement those contracts.
2. **Adapters must not call each other directly.** All coordination goes through services.
3. **Domain contains pure business logic.** No infrastructure imports.
4. **State is decoupled from data.** SQLite state DB survives DuckDB rebuilds.

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
