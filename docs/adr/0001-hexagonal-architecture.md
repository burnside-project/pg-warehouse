# ADR 0001: Hexagonal Architecture

**Date:** 2025-06-01
**Status:** Accepted

## Context

pg-warehouse must integrate with multiple infrastructure systems: PostgreSQL as a source, DuckDB as a warehouse, SQLite for state, and Parquet/CSV for exports. Future adapters (cloud stores, remote state, additional sources) are planned under the open-core model.

Tight coupling between business logic and any single infrastructure technology would make the system difficult to test, extend, and maintain.

## Decision

We adopt Hexagonal Architecture (ports and adapters). Domain logic depends only on port interfaces (`internal/ports/`). Concrete implementations live in `internal/adapters/`. Application services in `internal/services/` orchestrate workflows through port contracts.

Key rules:
- Ports define contracts; adapters implement them.
- Adapters must not call each other directly.
- Domain contains pure business logic with no infrastructure imports.
- All coordination flows through services.

## Consequences

- **Positive:** New adapters (e.g., S3 exporter, remote state store) can be added by implementing existing interfaces with zero changes to services or domain.
- **Positive:** Each adapter is testable in isolation using mock ports.
- **Positive:** Clear dependency direction makes the codebase navigable.
- **Negative:** More files and packages than a flat structure. Indirection requires developers to trace through interfaces.
- **Negative:** Port interfaces must be designed carefully; poor abstractions leak infrastructure concerns.
