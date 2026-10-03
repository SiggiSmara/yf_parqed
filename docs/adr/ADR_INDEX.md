# ADR Index

One row per ADR, oldest first. The folder an ADR sits in is its state; the `Status:` header inside the file is the canonical value and should agree with the folder.

| Date | ADR | State | Notes |
|------|-----|-------|-------|
| 2025-10-10 | [Yahoo Finance Data Pipeline](implemented/2025-10-10-yahoo-finance-data-pipeline.md) | implemented | Core Yahoo pipeline, ticker registry and daemon mode. |
| 2025-10-12 | [Partition-Aware Storage](implemented/2025-10-12-partition-aware-storage.md) | implemented | Monthly partitions per ticker; legacy and partitioned backends coexist. |
| 2025-10-12 | [Xetra Delayed Data](implemented/2025-10-12-xetra-delayed-data.md) | implemented | Phase 1 (raw trade capture) complete. OHLCV aggregation is tracked in its own ADR. |
| 2025-10-12 | [DuckDB Query Layer](idea/2025-10-12-duckdb-query-layer.md) | idea | Proposed; not scheduled. |
| 2025-12-05 | [OHLCV Aggregation Service](to-do/2025-12-05-ohlcv-aggregation-service.md) | to-do | Agreed; normalization (Phase 2a) is prioritized before aggregation. |
| 2025-12-06 | [Separation of Concerns](implemented/2025-12-06-separation-of-concerns.md) | implemented | Package split into `yahoo/`, `xetra/`, `common/`. |
| 2025-12-09 | [Normalized Analytics Layer](idea/2025-12-09-normalized-analytics-layer.md) | idea | Proposed; not scheduled. |
| 2026-04-26 | [Xetra Parser Schema Resilience](implemented/2026-04-26-xetra-parser-schema-resilience.md) | implemented | Multi-schema parser. The deferred quarantine steps were superseded by the raw cache (2026-05-01 ADR). |
| 2026-04-26 | [Xetra Two-Tier Trade Storage](to-do/2026-04-26-xetra-two-tier-storage.md) | to-do | Stable 7-column MiFIR core tier plus a flexible extended tier. |
| 2026-05-01 | [Xetra Daemon Write-Path Performance and Hygiene](implemented/2026-05-01-xetra-daemon-write-path-perf.md) | implemented | Raw cache, mini-file daily writes, monthly consolidation cadence. Follow-up on consolidation in the 2026-10-03 ADR. |
| 2026-10-03 | [Daemon Resource Footprint](to-do/2026-10-03-daemon-resource-footprint.md) | to-do | Xetra consolidation loop and memory, Yahoo full rewrite per cycle, nightly SIGKILL. Step A is due before 2026-11-01. |

## Folders

| Folder | Meaning |
|--------|---------|
| `idea/` | Exploratory; not agreed. |
| `to-do/` | Agreed; work not started. |
| `in-progress/` | Being implemented; the ADR's sequenced steps show what is left. |
| `implemented/` | Done. |
| `archived/` | Superseded. |

When an ADR changes state, move the file, update its `Status:` header, and update its row here.
