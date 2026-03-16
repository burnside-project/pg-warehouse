# Roadmap

## pg-warehouse Development Roadmap

### Phase 1 — Foundation (Current)

- [x] CLI with Cobra (init, sync, inspect, run, preview, export, doctor)
- [x] PostgreSQL connectivity with pgx/v5 connection pooling
- [x] Full snapshot sync
- [x] Watermark-based incremental sync
- [x] DuckDB local warehouse with typed table creation
- [x] SQL feature pipeline execution
- [x] Parquet and CSV export
- [x] SQLite state database (decoupled from warehouse)
- [x] Hexagonal architecture with clean port/adapter separation

### Phase 2 — CDC and Streaming

- [x] PostgreSQL logical replication via pglogrepl
- [x] Publication and replication slot management
- [x] Initial snapshot with consistent LSN
- [x] WAL event streaming (INSERT/UPDATE/DELETE)
- [x] Batched event application to DuckDB
- [x] LSN confirmation and progress tracking
- [x] Graceful shutdown with state persistence
- [ ] Connection recovery with exponential backoff
- [ ] Schema change detection and re-sync

### Phase 3 — Production Hardening

- [ ] Benchmarks and performance profiling
- [ ] Structured metrics and observability hooks
- [ ] Enhanced error handling and retry logic
- [ ] Checkpoint resume testing
- [ ] Soak testing for long-running CDC streams
- [ ] Plugin/connector expansion

### Phase 4 — Ecosystem

- [ ] Docker Compose quickstart with sample data
- [ ] Example pipelines (CDC, batch, feature engineering)
- [ ] Connector framework for custom sources/sinks
- [ ] Community contribution guides and templates
- [ ] Integration test suite with real PostgreSQL

---

## Enterprise Preview

The following capabilities are planned for [pg-warehouse Enterprise](https://burnsideproject.ai):

- Scheduling and orchestration
- S3/GCS/Iceberg cloud export
- Remote state store (multi-instance coordination)
- Distributed ingestion
- RBAC and governance
- Data lineage and feature registry
- Cloud dashboard and control plane
- Advanced alerting and notifications

Enterprise features extend the open-source core without modifying it.

---

*Roadmap items are subject to change based on community feedback and priorities.*
