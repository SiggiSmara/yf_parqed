# yf_parqed Roadmap

This roadmap captures planned and in-flight changes that do not yet live in the codebase. Each item should link to a feature-specific ADR that holds the detailed plan and work log.

## Completed

**Items are listed in reverse chronological order (newest first):**

- **Daemon Mode** (2025-12-05) — Production-ready continuous data collection for both Yahoo Finance and Xetra with trading hours awareness, timezone handling, PID management, graceful shutdown, and systemd integration. See `docs/DAEMON_MODE.md`.

- **Xetra Phase 1** (2025-10-19) — Raw per-trade data ingestion with empirically validated rate limiting, trading hours filtering, and intelligent CLI. 1,943 lines, 129 tests, 100% CLI coverage.

- [Partition-Aware Storage](adr/2025-10-12-partition-aware-storage.md) (2025-10-19) — Monthly Hive-style partitions, atomic writes, global run-lock, and migration CLI with parity verification.

## Upcoming Enhancements

- **Xetra Phase 2** — OHLCV aggregation (1m/1h/1d intervals) from raw trade data. Critical for drop-in compatibility with Yahoo Finance analytics workflows.

- [DuckDB Query Layer](adr/2025-10-12-duckdb-query-layer.md) — Optional analytics layer for zero-copy querying over partitioned parquet data.

- **Xetra Phase 3+** — Split tracking, multi-venue support (Tradegate), ISIN→ticker mapping via Deutsche Börse CSV, production hardening.

## Process Notes

- Keep entries ordered by likely delivery sequence.
- Once an ADR is marked "Accepted" and code ships, update this file to point to release notes or remove the entry if it becomes standard behavior.
- Use the ADR work logs to track partial implementations instead of duplicating status fields here.
