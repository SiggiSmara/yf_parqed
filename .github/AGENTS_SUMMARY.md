````markdown
# Agent Summary (migrated from `AGENTS.md`)

This file preserves the historical notes and compact coverage/achievement map previously kept at the repository root for agent and automation consumption. It is not the canonical runbook — operational guidance, safety rules and runbooks live under `.github/` and `docs/` and should be used for day-to-day operations.

## Service-Oriented Refactor (Completed)

- Completed: Service extraction (Steps 1.1–1.7) — COMPLETED ✅ (2025-10-11)
- Summary: `primary_class.py` was refactored into a thin façade plus specialized services: `ConfigService`, `TickerRegistry`, `IntervalScheduler`, `DataFetcher`, `StorageBackend` / `PartitionedStorageBackend`.
- Files: core service files live in `src/yf_parqed/` (see `ARCHITECTURE.md` for the detailed API and diagrams).

## Automated Coverage Map (compact)

- `tests/test_ticker_operations.py`: ticker registry lifecycle, interval status transitions, not-found maintenance, metadata integrity.
- `tests/test_storage_operations.py`: parquet merge/read helpers (dedupe, empty files, corruption cleanup).
- `tests/test_update_loop.py`: update-loop harness with mocks (rate limiter invocation, cooldowns, sequencing).
- `tests/test_cli.py`: Typer command smoke tests and wiring.
- `tests/test_cli_integration.py`: end-to-end CLI run (real `YFParqed` in temp workspace, mocked Yahoo responses).
- `tests/test_data_fetcher.py`: DataFetcher unit tests (windowing, interval constraints, normalization, error handling).
- `tests/test_storage_backend.py`: StorageBackend tests (read/save, corruption recovery, schema validation).

This map is a convenience index; the canonical testing guide and coverage policies are in `.github/TESTING_GUIDE.md`.

## Recently Completed (compact bullets)

- Rate-limiter stress testing: enforced burst/delay experiments — `tests/test_rate_limits.py` (2025-10-11).
- CLI option coverage: expanded Typer command test matrix — `tests/test_cli.py` (2025-10-11).
- Storage edge-case hardening: descending sequences, partial writes, dtype normalization — `tests/test_storage_operations.py` (2025-10-11).

## Future Improvements (summary)

- Partition-aware storage: minimize corruption blast radius via Hive-style partitions and atomic writes (see `docs/adr/2025-10-12-partition-aware-storage.md`).
- DuckDB query layer: zero-copy interactive querying on partitioned parquet for analytics.

## Where to look for authoritative guidance

- Policies & runbooks: `.github/DATA_SAFETY_STRATEGY.md`, `.github/DEVELOPMENT_GUIDE.md`.
- Architecture & service responsibilities: `ARCHITECTURE.md`.
- Release notes and roadmap: `docs/release-notes.md`, `docs/roadmap.md`.

````
