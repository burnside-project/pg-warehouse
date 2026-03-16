# ADR 0005: Open-Core Boundary

**Date:** 2025-06-01
**Status:** Accepted

## Context

pg-warehouse needs a sustainability model that keeps the core developer experience free and open while funding continued development. A clear, principled boundary between OSS and commercial features is essential to maintain community trust.

## Decision

Adopt an open-core model governed by a single rule:

> If it helps a developer work locally, it is open source.
> If it helps an organization operate in production, it is commercial.

**OSS scope:** CLI, YAML config, PostgreSQL connectivity, snapshot sync, watermark incremental sync, CDC via logical replication, DuckDB warehouse, table inspection, SQL feature execution, Parquet/CSV export, SQLite state management.

**Commercial scope:** Low-latency streaming, scheduling, cloud exports (S3/GCS/Iceberg), remote state, observability dashboards, RBAC/governance, lineage, fleet management.

The hexagonal architecture (ADR 0001) enforces this boundary cleanly: commercial features are new adapters behind existing port interfaces.

## Consequences

- **Positive:** Developers get a fully functional local analytics tool at no cost.
- **Positive:** The port/adapter boundary makes the OSS/commercial split architecturally clean.
- **Positive:** Commercial features do not require forking or modifying OSS code.
- **Negative:** The boundary must be maintained as features evolve; edge cases will need judgment calls.
