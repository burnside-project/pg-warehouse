# ADR 0003: SQLite for State Management

**Date:** 2025-06-01
**Status:** Accepted

## Context

pg-warehouse must track sync watermarks, CDC replication LSN positions, run history, audit logs, and execution locks. This state has a fundamentally different lifecycle from warehouse data: the DuckDB warehouse can be deleted and rebuilt from source at any time, but state (sync progress, watermarks) must survive those rebuilds.

Storing state inside DuckDB would entangle these lifecycles and make warehouse rebuilds destructive to operational state.

## Decision

Use SQLite (`modernc.org/sqlite`, pure-Go) for all state management, stored at `.pgwh/state.db` separately from the DuckDB warehouse file. The `StateStore` port interface defines the contract; `sqlitestate.Store` is the adapter.

State tables include: `sync_state`, `sync_history`, `cdc_state`, `feature_runs`, `audit_log`, `watermarks`, `lock_state`, `project_identity`, and `schema_version`.

## Consequences

- **Positive:** DuckDB warehouse files can be safely deleted and recreated without losing sync progress.
- **Positive:** SQLite is battle-tested for metadata workloads and requires zero configuration.
- **Positive:** Using `modernc.org/sqlite` (pure Go) avoids adding a second CGO dependency.
- **Negative:** Two embedded databases to reason about, each with its own file and connection lifecycle.
- **Negative:** Developers must understand which database holds which data.
