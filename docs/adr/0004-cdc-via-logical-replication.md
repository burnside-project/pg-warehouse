# ADR 0004: CDC via PostgreSQL Logical Replication

**Date:** 2025-06-01
**Status:** Accepted

## Context

Full snapshot sync is simple but does not scale for large tables that change infrequently. pg-warehouse needs a change data capture (CDC) mechanism to stream inserts, updates, and deletes from PostgreSQL in near-real-time.

The standard approach in the ecosystem is to deploy Debezium with Kafka, which introduces significant operational complexity (JVM, Kafka cluster, Connect workers, schema registry).

## Decision

Implement CDC using PostgreSQL native logical replication via `pglogrepl`. The `CDCSource` port interface defines operations for setup (publication + replication slot), initial snapshot, WAL streaming, LSN confirmation, and teardown. The adapter uses `pglogrepl` to speak the PostgreSQL replication protocol directly.

## Consequences

- **Positive:** Direct WAL access with no middleware. Lower latency than Debezium/Kafka pipelines.
- **Positive:** Zero additional infrastructure — only the existing PostgreSQL instance is needed.
- **Positive:** Consistent with the local-first, single-binary philosophy.
- **Negative:** Requires PostgreSQL server configuration (`wal_level = logical`, replication slot permissions).
- **Negative:** The replication protocol is complex; error handling and slot management must be robust.
- **Negative:** Only supports PostgreSQL as a source (not a general CDC framework).
