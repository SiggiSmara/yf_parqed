# yf_parqed Release Notes

This document records user-facing changes by release. Each section should capture the date, version tag, and a concise summary of highlights, breaking changes, migration notes, and upgrade guidance.

> **Process**
>
> - Update this file as part of the release checklist.
> - Keep entries short; link to ADRs or pull requests for deep dives.
> - Note any required migrations (e.g., storage format updates) and point to detailed runbooks.

## Unreleased

- **Xetra monthly consolidation no longer exhausts memory:** the consolidation now streams one trading day at a time and skips the work when the monthly file is already up to date. Previously it reloaded the whole previous month on every fetch cycle for the first days of each month, which filled RAM and swap on small hosts. See [ADR 2026-10-03](adr/in-progress/2026-10-03-daemon-resource-footprint.md).
- **Xetra monthly files are complete and say what they contain:** days left as unmerged mini-files are merged before a month is consolidated, a corrupt daily file is left out and logged instead of silently dropped, and a monthly file is never replaced by one with fewer trades. Each monthly file lists the days it contains in its Parquet metadata.
- **Xetra daily merge no longer deletes new data:** mini-files found next to an existing daily file are merged into it when they were staged after it; only genuinely stale ones are deleted. A mini-file whose column was stored with a wider type no longer blocks the merge of its whole day.
- **Integer contract for `price_notation`:** the column is always a nullable integer. A missing value is stored as null instead of turning the column into a float. A value that is not a whole number is stored as null (the field is a code, so it is not rounded), logged as an error, and the original file is kept under `contract_violations/` for later inspection.
- **Raw cache cleanup is stricter:** a raw file is deleted only when its own day is proven to be in a daily or monthly Parquet file.
- **Daemons exit promptly when the host is swapping:** after a stop request an idle daemon now ends within about a second of finishing its cleanup. Before, Python's exit read the whole swapped-out process back from disk, which could take longer than systemd's stop timeout and got the Xetra daemon killed. Applies to the Xetra, ISIN mapping and Yahoo daemons. All of them now also notice a stop request within 10 seconds while idle; some waits checked only once a minute.
- **Services no longer delete each other's PID files:** the systemd unit templates set `RuntimeDirectoryPreserve=yes` for the shared `/run/yf_parqed`. Before, stopping one service removed the directory, which also made a service starting at that moment fail with `Read-only file system`. **Upgrade note:** answer `y` when `daemon-manage.sh update` asks whether to reinstall the service templates.
- **Daemons stop between items instead of at the end of the cycle:** the Yahoo daemon checks for a stop request before each ticker, and the Xetra daemon before each date and file and between days of a monthly consolidation (an abandoned monthly file is not written; it is built when the month is triggered again, or with `consolidate-month`). The Xetra fetcher's 35-second burst cooldown, which is where most midnight stops used to land, and its 429 retry waits end early. Signal handlers now only set a flag; the main loop writes the `Received signal` line, which avoids loguru's `Could not acquire internal lock` message.
- **Log rotation no longer restarts the services:** the logrotate config uses `copytruncate` instead of a `postrotate` that restarted both daemons at midnight and killed the cycle in progress. `daemon-manage.sh update` replaces an installed config that still has the `postrotate`.
- **Yahoo cycles touch only the months they fetch:** with partitioned storage, saving a ticker now opens only the monthly files that the fetched bars fall into (one, or two around a month change, instead of every month the ticker has) and rewrites a file only when its content changes. A cycle that refetches bars it already has writes nothing, and a ticker whose fetch returns nothing is not opened at all. Captured bars are not replaced by less: an incoming bar that holds fewer values than the stored bar for the same time is ignored (with a warning), and a month that would lose a stored bar is not written. The legacy single-file layout is unchanged and gets none of this. **Behaviour change:** an unreadable partition in a month that is not being updated no longer blocks the update and is no longer deleted by the recovery logic, and the daemon no longer notices it. See [ADR 2026-10-03](adr/in-progress/2026-10-03-daemon-resource-footprint.md).
- **Upgrade note:** after deploying, rebuild the monthly files once with `xetra-parqed consolidate-month DETR --all` (runbook in the ADR). Four existing monthly files are behind their daily data.


## 2025-12-06 — Version 0.4.2 (UTC Trading Hours Hardening)

- **UTC-first trading hours:** TradingHoursChecker now evaluates open/close windows in UTC, logging both market-local and UTC windows to avoid timezone drift and DST confusion.
- **Daemon scheduling safeguards:** Yahoo and Xetra daemons cap sleeps at market close to avoid skipping late-session cycles; overnight windows are handled correctly.
- **Timezone overrides for Xetra:** `xetra-parqed fetch-trades` now accepts `--market-timezone` and `--system-timezone`, aligning with the Yahoo CLI flexibility.


## 2025-12-06 — Version 0.4.1 (Shim Removal & Package Split)

 - **Separation of Concerns (completion):** Finalized the separation-of-concerns work that began in the previous release: consolidated service boundaries, removed legacy shims, and completed package reorganization so core services (`ConfigService`, `TickerRegistry`, `IntervalScheduler`, `DataFetcher`, `StorageBackend`) are decoupled and importable from canonical modules. See `docs/adr/2025-12-06-separation-of-concerns.md` for details and migration guidance.
- **Package separation completed:** Removed legacy shim modules now that all imports target `yf_parqed.common`, `yf_parqed.yahoo`, and `yf_parqed.xetra` directly. Tests updated to canonical paths; 402/402 passing.
- **CLI entrypoints updated:** `yf-parqed` now resolves to `yf_parqed.yahoo.yfinance_cli:app` and `xetra-parqed` to `yf_parqed.xetra.xetra_cli:app`, aligning exposed endpoints with the new package layout.
- **Metadata:** Bumped version to 0.4.1 to reflect the structural change. No behavioral changes expected beyond import/entrypoint paths.

---

## 2025-12-05 — Version 0.4.0 (Daemon Mode)

- **Separation of Concerns (initiation):** Began a formal separation-of-concerns effort (see `docs/adr/2025-12-06-separation-of-concerns.md`) to clarify service boundaries between CLI, fetchers, and storage. Work carried across the 0.4.x patch releases and completed in 0.4.1.
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
