# ADR 2025-10-12: Partition-Aware Storage

## Status

Proposed (2025-10-12)

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
   - Introduce a `PartitionPathBuilder` that maps `(interval, ticker, timestamp)` to `stocks_<interval>/ticker=<TICKER>/year=<YYYY>/month=<MM>/day=<DD>/data.parquet`.
   - Zero-pad month/day to keep lexical ordering and compatibility with common query engines.

2. **Feature Flag**
   - Extend configuration with a `storage.partitioned` toggle (default `false`).
   - On startup, detect existing layout:
     - Partitioned directories present (and no legacy files) ⇒ enable the flag automatically.
     - Legacy files present ⇒ keep flag disabled and emit a reminder to run the migration.
     - Mixed layouts ⇒ stay in legacy mode, warn about completing migration before switching.
   - When the flag is `false`, continue using the legacy flat files. When `true`, use the partition-aware implementation.

3. **Storage Backend Extensions**
   - Implement a partition-aware backend variant that determines the affected partitions from the incoming data (or metadata) and handles merging/writing internally—no orchestration changes in the façade required.
   - Legacy mode keeps the current single-file implementation. Partitioned mode applies only to high-volume intervals (`1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`); wider intervals (`5d`, `1wk`, `1mo`, `3mo`) remain on the legacy storage to avoid sparse partition overhead.
   - Preserve existing schema validation, deduplication, and corruption recovery logic in both modes.
   - Prior to switching the backend, refactor the façade to derive `start_date` from registry metadata (`last_data_date`) instead of rereading persisted files, so the partitioned backend is never forced to aggregate historical data just to discover the latest timestamp.

4. **Migration Command**
   - Add a Typer CLI command `partition-migrate` that:
   1. Estimates disk requirements up front (legacy footprint ×2 plus overhead) and prompts the user to proceed, allowing them to skip the backup if they explicitly acknowledge the risk.
   2. Creates a timestamped backup of the current `stocks_<interval>/` directories when the user accepts (default).
   3. Writes partitioned files to a sibling root (e.g., `stocks_<interval>_partitioned/`).
   4. Verifies row counts (and optional checksums) between legacy and partitioned data.
   5. Offers a dry-run mode and the ability to resume after interruption.
   6. Switches the active storage root only after verification passes.
   - Provide a complementary rollback command to restore from the backup if needed.

5. **Testing Plan (TDD)**
   - Add failing unit tests for `PartitionPathBuilder` covering padding, directory ordering, and file naming.
   - Add storage backend tests that exercise partitioned `read`/`save`, corruption recovery, and mixed-mode safety.
   - Extend integration tests (`tests/test_cli_integration.py`, `tests/test_update_end_to_end.py`) to run once with partitioned storage enabled.
   - Create migration command tests using temporary directories to verify backup, verification, and rollback behavior.

6. **Documentation**
   - Update `ARCHITECTURE.md` and `README.md` once the feature is available, including operational guidance and migration warnings.

## Alternatives Considered

- **Single parquet per month**: Reduces file count but still exposes large failure domains and makes incremental backups harder.
- **SQLite/DuckDB storage**: Would simplify querying but introduces a new dependency and porting risk; better addressed separately via the DuckDB ADR.

## Work Log

| Date       | Milestone | Status     | Notes |
|------------|-----------|------------|-------|
| 2025-10-12 | Draft TDD plan | Pending | Initial ADR created. |
|            | Path builder & tests | Pending | |
|            | Partitioned backend implementation | Pending | |
|            | Migration CLI + rollback | Pending | |
|            | Docs + final rollout | Pending | |

## Consequences

- Expect increased inode/file counts; document infrastructure requirements.
- Backup size may decrease due to smaller incremental deltas, but more files could affect tar/zip operations.
- Need to monitor performance impact; more files might reduce per-write throughput, but we trade it for resilience.
