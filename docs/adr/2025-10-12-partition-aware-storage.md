# ADR 2025-10-12: Partition-Aware Storage

## Status

Accepted (2025-10-12)

## Context

- `StorageBackend` currently writes a single parquet file per ticker/interval (e.g., `stocks_1d/AAPL.parquet`).
- Years of accumulated data make any corruption or partial write risky; recovery requires rewriting the whole file.
- Backups and selective rewrites are expensive because files grow without bound.
- Future analytics (DuckDB, Spark) benefit from Hive-style directory partitions (`key=value`).

## Decision Drivers

1. Reduce the blast radius of file corruption or interrupted writes.
2. Enable incremental backups and selective data rewrites.
3. Maintain compatibility with existing CLI commands and regression suite during rollout.
4. Provide a reversible migration path that preserves historical data.

## Proposed Approach

1. **Path Strategy**
   - Introduce a `PartitionPathBuilder` that maps `(market, source, interval, ticker, timestamp)` to a venue-first layout such as `<market>/<source>/stocks_<interval>/ticker=<TICKER>/year=<YYYY>/month=<MM>/day=<DD>/data.parquet` (for example, `us/yahoo/stocks_1d/AAPL/...`, `de/xetra/stocks_1m/DBK/...`).
   - Zero-pad month/day to keep lexical ordering and compatibility with common query engines.
   - Allow the builder to collapse to legacy two-segment paths (`stocks_<interval>/...`) when `market` or `source` metadata is unavailable to support older datasets during the transition.

2. **Storage Backend Extensions**
   - Implement against the shared `StorageInterface` so legacy and partitioned backends remain swappable without façade changes.
   - Leverage `PartitionPathBuilder` to derive Hive-style paths; if required metadata is absent, fail fast so the caller can switch to the legacy backend explicitly.
   - Harmonize directory layout around a venue-first root (e.g., `us/yahoo/stocks/<interval>/...`, `de/xetra/stocks/<interval>/...`, `de/tradegate/stocks/<interval>/...`) so each data source lands in its dedicated tree.
   - Permit the backend to emit multiple dataset flavors under a single `(market, source)` root (for example, `raw_trades/` and `aggregated_bars/`) to support feeds that deliver tick-level data alongside derived bars.
   - Partitioned mode applies only to high-volume intervals (`1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`); wider intervals (`5d`, `1wk`, `1mo`, `3mo`) remain on the legacy storage to avoid sparse partition overhead. The interval list is configurable per data source.
   - Preserve existing schema validation, deduplication, and corruption recovery logic in both modes. When a partition file fails to load, delete it, fail fast, and surface a clear error so the operator can re-fetch the data; optionally emit warnings about detected gaps after a successful read.
   - Determine affected partitions from incoming data and only write those files; when partitioning is disabled for a ticker/venue, route the request through the legacy backend instead of silently falling back.
   - Record storage metadata (`storage_root`, `dataset_flavor`, interval hints) after successful saves so downstream jobs resolve paths without rescanning.
   - Keep configuration global: the partition backend operates in partitioned mode for all tickers once enabled, otherwise the legacy backend handles everything. ✅ Implemented. The façade now instantiates either backend via `_create_storage_backend`, wiring `PartitionPathBuilder` automatically.
   - Cover partition path resolution, mixed-mode behavior, dataset separation, and corruption handling with unit + integration tests. ✅ Covered by dedicated backend/unit suites and the update loop harness.
   - Skip manifest caching in the first iteration; monitor performance and revisit after DuckDB integration if repeated globbing becomes a measurable bottleneck.

3. **Feature Flag**
    - Extend configuration with a `storage.partitioned` toggle (default `false`) plus optional per-market or per-source overrides (for example, enable partitioning for `de/xetra` ahead of `us/yahoo`). ✅ Implemented via `storage_config.json`; `ConfigService` exposes setters and normalization, while tests assert precedence rules and persistence.
    - The façade consults `ConfigService.is_partitioned_enabled(...)` at runtime, selecting the appropriate backend before each update loop. ✅ Exercised via `tests/test_update_loop.py::test_partitioned_backend_selected_when_flag_enabled`.
   - On startup, detect existing layout for each `(market, source)` pair and activate partition mode automatically once migrations verify all intervals. ✅ Migration writes per-source partition flags after checksum parity, and startup reuses those flags instead of rescanning the filesystem.
   - When a pair remains in legacy mode, continue using flat files without impacting other sources that have completed migration. ✅ CLI toggles for manual overrides are available via the main CLI (`partition-toggle`).

4. **Migration Command**
    - ✅ Introduced a Typer CLI command `partition-migrate` with `init`, `status`, `mark`, and `migrate` subcommands. The workflow persists plan changes, enforces a single legacy location, defaults the venue to `us:yahoo`, and lets operators select intervals by number or name (including an `--all` batch mode).
   - ✅ Migration runs now capture job counts, legacy/partition row totals, per-interval checksum parity, and verification metadata after each ticker so progress is persisted across invocations.
    - ⏳ Remaining enhancements to reach the originally proposed behavior:
        1. ✅ Estimate disk requirements up front (legacy footprint ×2 plus overhead) for each `(market, source)` pair and prompt the user before copying (CLI now blocks migrations when free space is insufficient).
        2. ✅ Replace ad-hoc backups with verification-by-default. The CLI no longer creates backups; instead operators rely on row-count and checksum validation plus optional `--delete-legacy` cleanup once parity is confirmed.
        3. ✅ Enforce legacy data residency under `data/legacy` and ensure partitioned outputs land under the venue-specific `data/<market>/<source>/...` hierarchy, preventing accidental co-location of legacy and partitioned files.
        4. ✅ Compare row counts and per-ticker checksums between legacy and partitioned data, failing the migration when mismatches are detected.
        5. ⏳ Offer a dry-run mode and resume support that tracks completion status for each `(market, source, dataset)` tuple.
        6. ✅ Switch the active storage roots only after verification passes for that tuple (per-source partition flag toggled automatically once all venue intervals verify).
        7. ✅ Backfill ticker metadata (per-interval `storage` blocks) so post-migration runs can resolve the correct paths without rescanning legacy layouts.
   - ⏳ Provide a complementary rollback command to restore from the backup if needed, scoped to the affected `(market, source, dataset)` entries.

5. **Testing Plan (TDD)**
   - Add failing unit tests for `PartitionPathBuilder` covering padding, directory ordering, file naming, and fallback behavior when `market`/`source` metadata is missing.
   - Add storage backend tests that exercise partitioned `read`/`save`, corruption recovery, mixed-mode safety, and multiple dataset flavors (bars plus raw trades).
   - Extend integration tests (`tests/test_cli_integration.py`, `tests/test_update_end_to_end.py`) to run once per data source with partitioned storage enabled.
   - Create migration command tests using temporary directories to verify backup, verification, resume, and rollback behavior across multiple `(market, source, dataset)` combinations.

6. **Documentation**
   - Update `ARCHITECTURE.md` and `README.md` once the feature is available, including operational guidance and migration warnings.

## Alternatives Considered

- **Single parquet per month**: Reduces file count but still exposes large failure domains and makes incremental backups harder.
- **SQLite/DuckDB storage**: Would simplify querying but introduces a new dependency and porting risk; better addressed separately via the DuckDB ADR.

## Work Log

| Date       | Milestone | Status     | Notes |
|------------|-----------|------------|-------|
| 2025-10-12 | Draft TDD plan | Completed | Initial ADR created. |
| 2025-10-12 | Path builder & tests | Completed | `PartitionPathBuilder` implemented with legacy fallback and zero-padded Hive-style directories. |
| 2025-10-12 | Partitioned backend implementation | Completed | `PartitionedStorageBackend` added with dedicated tests; façade upgraded to accept legacy paths or `StorageRequest` instances transparently. |
| 2025-10-12 | Feature flag infrastructure | Completed | `ConfigService` persists `storage.partitioned` (with per-market/source overrides) and the façade swaps backends dynamically; exercised by config + update loop tests. |
| 2025-10-15 | Migration CLI usability enhancements | Completed | Added default venue resolution, interactive interval prompts, `--all` batch option, and expanded CLI regression tests. |
| 2025-10-15 | Legacy-path enforcement & verification | Completed | CLI now requires legacy data under `data/legacy`, removes backup flow, surfaces disk estimates, and validates migrations with row-count + checksum parity before completion. |
| 2025-10-15 | Feature flag runtime integration | Completed | Migration backfills per-interval storage metadata, auto-activates partition mode after venue verification, and the update loop routes traffic through the partitioned backend when metadata is present (covered by update-loop harness tests). |
| 2025-10-15 | Partition CLI toggles | Completed | Added Typer command `partition-toggle` to enable, disable, or clear partition overrides for global, market, or source scopes. |
|            | Rollback command | Pending | |
|            | Docs + final rollout | Pending | |

## Consequences

- Expect increased inode/file counts; document infrastructure requirements.
- Backup size may decrease due to smaller incremental deltas, but more files could affect tar/zip operations.
- Need to monitor performance impact; more files might reduce per-write throughput, but we trade it for resilience.
- Adjusting directory layout requires a migration plan for existing `stocks_<interval>` paths; the migration CLI must handle renaming/moving into the venue-first hierarchy while preserving backward compatibility.
- Cross-source queries gain clarity because datasets live under distinct roots, but operators must provision monitoring per source (e.g., disk usage alerts per venue tree).
