# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for pg-warehouse.

ADRs document significant architectural decisions along with their context and consequences. They provide a historical record of why the system is shaped the way it is.

## Format

Each ADR follows the format proposed by [Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions):

- **Status** — Proposed, Accepted, Deprecated, or Superseded
- **Context** — The forces at play and the problem being addressed
- **Decision** — What we decided to do
- **Consequences** — What happens as a result, both positive and negative

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-hexagonal-architecture.md) | Hexagonal Architecture | Accepted |
| [0002](0002-duckdb-as-local-warehouse.md) | DuckDB as Local Warehouse | Accepted |
| [0003](0003-sqlite-for-state-management.md) | SQLite for State Management | Accepted |
| [0004](0004-cdc-via-logical-replication.md) | CDC via Logical Replication | Accepted |
| [0005](0005-open-core-boundary.md) | Open-Core Boundary | Accepted |
| [0006](0006-single-binary-distribution.md) | Single Binary Distribution | Accepted |

## Creating a New ADR

Copy an existing ADR and increment the number. Use a short, descriptive filename. Update the index above.
