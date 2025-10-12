# yf_parqed Release Notes

This document records user-facing changes by release. Each section should capture the date, version tag, and a concise summary of highlights, breaking changes, migration notes, and upgrade guidance.

> **Process**
>
> - Update this file as part of the release checklist.
> - Keep entries short; link to ADRs or pull requests for deep dives.
> - Note any required migrations (e.g., storage format updates) and point to detailed runbooks.

## Unreleased

- Planned: Partition-aware storage rollout ([ADR 2025-10-12](adr/2025-10-12-partition-aware-storage.md))
- Planned: Optional DuckDB analytics layer ([ADR 2025-10-12](adr/2025-10-12-duckdb-query-layer.md))

---

## 2025-10-12 — Documentation Restructure

- Added `docs/roadmap.md` and feature-specific ADRs to track partition-aware storage and DuckDB analytics enhancements.
- Established this release notes log to centralize future change summaries.

## 2025-10-11 — Version 0.2.0 (Service-Oriented Refactor)

- Rebuilt `YFParqed` into a façade over extracted services (`ConfigService`, `TickerRegistry`, `IntervalScheduler`, `DataFetcher`, `StorageBackend`).
- Achieved parity with legacy behavior while expanding test coverage to 109 cases across unit and integration layers.
- Introduced rate-limiter stress tests, CLI option coverage, and enhanced storage edge-case handling.

## 2024-12-26 — Version 0.1.0 (Initial MVP)

- Delivered the first working CLI to initialize tickers, fetch data from Yahoo Finance, and persist per-interval parquet files.
- Implemented basic tracking of ticker status (`active` vs `not_found`) and JSON-backed metadata storage.
- Laid groundwork for automated updates and not-found maintenance workflows.
