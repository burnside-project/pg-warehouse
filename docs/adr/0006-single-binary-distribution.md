# ADR 0006: Single Binary Distribution

**Date:** 2025-06-01
**Status:** Accepted

## Context

Developer tools that require installing runtimes, managing dependencies, or running background services face adoption friction. pg-warehouse targets individual developers who want to sync PostgreSQL data locally with minimal setup.

## Decision

Distribute pg-warehouse as a single Go binary. DuckDB and SQLite are embedded via Go bindings (`marcboeker/go-duckdb` and `modernc.org/sqlite`). The CLI is built with Cobra. Installation is `brew install` or a single binary download — no Docker, no JVM, no Python environment.

Release builds use GoReleaser to produce platform-specific binaries with semantic versioning.

## Consequences

- **Positive:** `curl | tar` or `brew install` gets a developer from zero to syncing in under a minute.
- **Positive:** No runtime dependencies to manage or version-conflict with.
- **Positive:** Reproducible builds via GoReleaser across Linux, macOS, and Windows.
- **Negative:** CGO is required for go-duckdb, which complicates cross-compilation. CI must build on each target platform or use cross-compilation toolchains.
- **Negative:** Binary size is larger than a pure-Go tool due to embedded C libraries.
