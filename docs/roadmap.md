# yf_parqed Roadmap

This roadmap captures planned and in-flight changes that do not yet live in the codebase. Each item should link to a feature-specific ADR that holds the detailed plan and work log.

## Upcoming Enhancements

- [Partition-Aware Storage](adr/2025-10-12-partition-aware-storage.md) — restructure per-interval data into Hive-style partitions for safer persistence and backup workflows.
- [DuckDB Query Layer](adr/2025-10-12-duckdb-query-layer.md) — add an optional analytics layer for zero-copy querying over historical parquet output.
- [Xetra Delayed Data Ingestion](adr/2025-10-12-xetra-delayed-data.md) — integrate Deutsche Börse delayed market data for German equities.

## Process Notes

- Keep entries ordered by likely delivery sequence.
- Once an ADR is marked "Accepted" and code ships, update this file to point to release notes or remove the entry if it becomes standard behavior.
- Use the ADR work logs to track partial implementations instead of duplicating status fields here.
