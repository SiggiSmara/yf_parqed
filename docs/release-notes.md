# yf_parqed Release Notes

This document records user-facing changes by release. Each section should capture the date, version tag, and a concise summary of highlights, breaking changes, migration notes, and upgrade guidance.

> **Process**
>
> - Update this file as part of the release checklist.
> - Keep entries short; link to ADRs or pull requests for deep dives.
> - Note any required migrations (e.g., storage format updates) and point to detailed runbooks.

## Unreleased

- Planned: Optional DuckDB analytics layer ([ADR 2025-10-12](adr/2025-10-12-duckdb-query-layer.md))
- Planned: Xetra Phase 2 OHLCV aggregation (1m/1h/1d intervals)

---

## 2025-12-05 — Version 0.4.0 (Daemon Mode)

- **Yahoo Finance Daemon Mode**: Production-ready continuous data collection with NYSE/NASDAQ trading hours awareness (09:30-16:00 US/Eastern, optional extended hours 04:00-20:00). Features include automatic timezone conversion, configurable ticker maintenance (daily/weekly/monthly), PID file management, graceful shutdown (SIGTERM/SIGINT), and systemd integration. Full documentation in `docs/DAEMON_MODE.md`.

- **Xetra Daemon Mode Complete**: Enhanced Xetra CLI with daemon mode supporting 08:30-18:00 CET/CEST trading hours, PID management, file logging with rotation, signal handling, and systemd integration. Added 52 daemon-specific tests (18 integration + 34 trading hours). Complete production deployment guide available.

- **CLI Simplifications**: Xetra `fetch-trades` command now features intelligent date detection (auto-determines missing data), storage-by-default behavior, and simplified parameter set. Reduced from 5 required parameters to 1 (`--venue`). Added `check-status` diagnostic command.

- **Testing**: Expanded test coverage with daemon lifecycle tests, trading hours validation across timezones, and PID file management edge cases. Total: 183 Yahoo Finance tests + 129 Xetra tests.

  **Migration Notes**:
  - Daemon mode is opt-in via `--daemon` flag; existing cron-based workflows unaffected
  - Trading hours default to NYSE regular hours (YF) and Xetra hours (Xetra); use `--trading-hours` or `--extended-hours` flags to customize
  - PID file location configurable; production deployments should use `/run/<service>/` via systemd `RuntimeDirectory`

---

## 2025-10-19 — Version 0.3.1 (Partition-Aware Storage + Xetra Foundation)

- Completed the Partition-Aware Storage ADR and shipped operational safeguards: monthly Hive-style partitions, same-dir temp writes with fsync + atomic replace, a mkdir-based global run-lock with operator cleanup tooling, and a migration CLI that verifies parity before toggling the runtime to partitioned mode. Full test suite passed locally (177 tests).

- **Xetra Delayed Data Foundation (Phase 1 Complete)**: Delivered production-ready infrastructure for Deutsche Börse Xetra 15-minute delayed trade data ingestion. Added `xetra-parqed` CLI with 5 commands (`fetch-trades`, `check-status`, `list-files`, `check-partial`, `consolidate-month`) and 100% test coverage. Implemented empirically validated rate limiting (0.6s/30req/35s cooldown, R²=0.97, zero 429 errors over 810 files) and trading hours filtering (56.5% file reduction). Raw per-trade data storage operational with daily partitions (`venue=VENUE/year/month/day/`) and monthly consolidation. Core services: XetraFetcher (97% coverage), XetraParser (100% coverage), XetraService (80% coverage). Total: 1,943 lines, 129 Xetra-specific tests. **Note**: OHLCV aggregation (1m/1h/1d intervals) pending Phase 2 implementation. See [Xetra ADR](adr/2025-10-12-xetra-delayed-data.md) for details.

  **Migration Notes**:
  - The migration CLI supports plan persistence, per-venue verification, and `--non-interactive` automation-friendly runs. Operators should run the `partition-migrate status` command prior to any destructive actions and may use `partition-toggle` to control rollout scope.
  - The `xetra-parqed` CLI is fully operational for raw trade data collection. OHLCV interval generation (Phase 2) required for drop-in compatibility with Yahoo Finance analytics workflows.

---

## 2025-10-15 — Version 0.3.0 (Partition Storage Rollout Prep)

- Advanced the partition-aware storage rollout ([ADR 2025-10-12](adr/2025-10-12-partition-aware-storage.md)) with migration CLI refinements—defaulted venue selection, numeric-or-name prompts, and an `--all` batch mode that records progress back into the plan—while noting disk estimation, resume/backfill, and rollback steps remain outstanding.
- Updated the ADR work log to capture the delivered CLI usability milestone and enumerate the remaining migration workflow gaps before full rollout.

---

## 2025-10-12 — Version 0.2.1 (Documentation Restructure)

- Added `docs/roadmap.md` and feature-specific ADRs to track partition-aware storage and DuckDB analytics enhancements.
- Established this release notes log to centralize future change summaries.
- No code changes; documentation and planning updates only.

---

## 2025-10-11 — Version 0.2.0 (Service-Oriented Refactor)

- Rebuilt `YFParqed` into a façade over extracted services (`ConfigService`, `TickerRegistry`, `IntervalScheduler`, `DataFetcher`, `StorageBackend`).
- Achieved parity with legacy behavior while expanding test coverage to 109 cases across unit and integration layers.
- Introduced rate-limiter stress tests, CLI option coverage, and enhanced storage edge-case handling.

## 2024-12-26 — Version 0.1.0 (Initial MVP)

- Delivered the first working CLI to initialize tickers, fetch data from Yahoo Finance, and persist per-interval parquet files.
- Implemented basic tracking of ticker status (`active` vs `not_found`) and JSON-backed metadata storage.
- Laid groundwork for automated updates and not-found maintenance workflows.
