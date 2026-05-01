# ADR 2026-05-01: Xetra Daemon Write-Path Performance and Hygiene

## Status: Implemented

## Context

The Xetra capture pipeline now runs as a long-lived systemd daemon under `yfparqed`, processing ~800 posttrade files per trading day per venue. A code review of the end-to-end capture path (parser → service → storage backend) surfaced several O(n²) hot loops, a silent correctness regression introduced by the legacy-column migration, and a number of smaller hygiene issues. None block today's operation, but together they impose growing per-file cost as the day progresses, and one of them (item #5 below) actively degrades resume-detection on migrated files.

This ADR is motivated by the daemon use case specifically: failures and inefficiencies that are tolerable in a one-shot CLI become much more visible when the same code runs every minute for months at a time.

## Decision

Address the issues below in the priority order given. Items 1, 2, 4 form a coherent redesign of the write path and should be designed together; the rest are independent.

### 1. `save_xetra_trades()` — eliminate O(n²) intra-day rewrite *(highest impact)*

**File:** `src/yf_parqed/common/partitioned_storage_backend.py` (`save_xetra_trades`, lines 22–87)

Each of ~800 minute-files per day reads the entire accumulated daily Parquet, concatenates one minute of new rows, and rewrites the whole file. By end of day the 800th write is reading ~440K rows and writing them back for a ~1K-row append. Total work for one trading day ≈ O(n²) ≈ ~160 million rows of read+write.

**Recommended fix:** append-only writes via `pyarrow.parquet.ParquetWriter`. Open one writer per (venue, day), each minute writes one row group, close at end of day. This:

- Bounds memory to one row group instead of the entire accumulated day
- Eliminates the read-merge-rewrite cycle entirely
- Keeps the existing "one file per day" invariant downstream consumers depend on
- Avoids the same mixed-encoding hazard already fixed in the migration path (single writer, single encoding choice — set `use_dictionary=False` to be explicit)

Alternatives considered:
- **Per-minute files + lazy daily roll-up**: simpler write, but balloons file count and forces a separate roll-up step.
- **In-memory buffer with periodic flush**: simple but loses data on daemon crash; rejected on data-safety grounds.

The append-only `ParquetWriter` approach requires deciding how to handle the (rare) case where the daemon restarts mid-day: we either re-open the existing file's writer (PyArrow does not support this directly), or accept that a restart starts a new sibling file for the rest of the day and consolidate on close. The roll-up at end-of-day or end-of-month already exists and would naturally absorb sibling files.

**Note:** The raw JSON cache (item #4 revised decision below) substantially reduces the urgency of this item. A SIGKILL mid-write no longer means data loss since raw files are persisted before parsing. The O(n²) cost remains real and should be fixed, but it is no longer the top safety priority.

### 2. `_consolidate_to_monthly()` — run once per month, not after every successful date

**File:** `src/yf_parqed/xetra/xetra_service.py` (`_consolidate_to_monthly`, lines 687–804; called from lines 642–654)

After every completed date, the service re-reads **all** daily files for the month, concatenates, and rewrites the monthly file. On the 22nd day of a month that re-loads 21 already-consolidated days. Same O(n²) shape as item #1, at month scale.

**Recommended fix:** trigger consolidation once per month (when the month rolls over, or on first daemon iteration of a new month), not after each successful date. Alternative: append the new day's row group to the existing monthly file via `ParquetWriter` rather than reading-and-rewriting.

### 3. `get_missing_dates()` — dead branching, always returns every available date

**File:** `src/yf_parqed/xetra/xetra_service.py` (`get_missing_dates`, lines 155–187)

The if/else block conditions on whether the daily Parquet exists, but both branches `append`. The "incremental logic will check which files are already stored" comment is honest but the differentiation is meaningless. Cheap cleanup; collapse to an unconditional append, or actually use the existence check to skip dates that don't need re-checking.

### 4. Download log — replaced by raw JSON cache *(revised)*

**Original finding:** `pd.read_parquet → concat → drop_duplicates → to_parquet` on the entire centralized download log, every 10 files — O(n²) at month scale.

**Revised decision (2026-05-01):** Drop the centralized download log entirely. Replace it with a raw JSON cache that serves double duty: data safety and resume detection.

Every downloaded `.json.gz` is persisted to:
```
{root}/de/xetra/raw/{venue}/year=YYYY/month=MM/day=DD/{filename}.json.gz
```
before it is parsed. Resume detection becomes a simple `path.exists()` check — no Parquet read, no timestamp extraction, no download log rewrite. After 7 days, cached files are cleaned up only when a readable daily or monthly Parquet confirms the data is preserved.

This supersedes the original "fallback path" approach (inferring downloaded timestamps from `trading_date_time` in the trades Parquet). The raw cache is structurally simpler, works for empty files that produce no Parquet rows, and survives SIGKILL mid-write.

**Implementation status (complete):** `_raw_cache_path`, `_save_to_raw_cache`, `_is_cached`, `_is_parquet_readable` added. `fetch_and_parse_trades` saves to cache before parsing. Resume detection in `fetch_and_store_missing_trades_incremental` replaced with `_is_cached` check. Download log write path removed. `cleanup_raw_cache` and `reprocess_from_raw_cache` implemented. CLI commands `cleanup-raw-cache` and `reprocess-raw-cache` added. Tests in `tests/test_xetra_raw_cache.py` (22 tests) and `TestRawCacheTracking` in `tests/test_xetra_service.py`.

### 5. `trade_time` column reference — silent regression after legacy migration *(implemented)*

**File:** `src/yf_parqed/xetra/xetra_service.py`

After the 2025-legacy → MiFIR column migration, the column is `trading_date_time`. The reference to `trade_time` raised on every migrated file; the surrounding `try/except` swallowed it silently, causing the daemon to re-download files it already had.

**Fix applied:** renamed to `trading_date_time`. The raw cache (item #4) now makes this the secondary resume signal anyway — the primary signal is raw file existence.

### 6. `XetraFetcher.client` — single `httpx.Client` for the lifetime of the daemon

**File:** `src/yf_parqed/xetra/xetra_fetcher.py` (line 73)

httpx connections can go stale on long-running daemons (TCP keepalive, proxy timeouts, DNS rotation). No reported failures yet, but worth either:
- Periodically rotating the client at a low-traffic boundary (e.g., between daemon iterations)
- Adding retry-on-`RemoteProtocolError`/`ConnectError` in the download path

### 7. Hard-coded `"DETR-posttrade-"` filename parsing *(implemented)*

**File:** `src/yf_parqed/xetra/xetra_service.py`

`filename.split("DETR-posttrade-")[1]` silently breaks for DFRA/DGAT/DEUR. **Fix applied:** replaced with `filename.split(f"{venue}-posttrade-")[1]` at all call sites. This bug is now moot in the resume path since raw cache lookup uses the full filename directly.

### 8. Sequential download/parse/store — missed concurrency opportunity

**File:** `src/yf_parqed/xetra/xetra_service.py` (line 539)

The per-file loop is strictly serial: download → parse → store → next download. Download is rate-limited and sleep-bound (~0.6s between requests + 35s burst cooldown every 30 files). Parse and store are CPU/I/O bound. Running parse+store concurrently with the next download via a small queue would yield an estimated 30–40% throughput gain on catch-up runs, with no impact on steady-state.

Worth doing only if catch-up speed matters. Given we just had a 40-day outage where catch-up speed mattered a lot, the answer is probably yes — but lower priority than items 1, 2, 4.

### 9. `import` statements inside hot loops

**File:** `src/yf_parqed/xetra/xetra_service.py`

`import pandas as pd` and similar appeared inside per-file loops. Mostly removed as a side-effect of the download log cleanup. Any remaining instances should be hoisted to module top.

### 10. `XetraParser.parse()` — pandas detour for small JSONL files

**File:** `src/yf_parqed/xetra/xetra_parser.py` (lines 108–109)

`pd.DataFrame(trades)` followed by rename works fine for ~1000-row files but is a needless pandas allocation. Building a PyArrow table directly via `pa.Table.from_pylist(...)` would be cheaper and align the parser output type with the storage layer's preferred input. Low priority unless file sizes grow significantly.

### 11. `find_unmigrated_files()` — runs `pq.read_schema()` on every file on every daemon iteration

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 1037–1063)

The migration preflight scans the entire venue tree on every daemon startup. Once a file is migrated it stays migrated; there's no need to re-check. Add a sentinel file (`.migration_complete`) or persist the result, and skip the scan when it's set.

## Sequenced Steps

- [x] **Step A** — Fix `trade_time` → `trading_date_time` reference (item #5) and hard-coded `"DETR-posttrade-"` venue prefix (item #7). Independent correctness fixes; committed alone.
- [x] **Step B** — Implement raw JSON cache: add `_raw_cache_path`, `_save_to_raw_cache`, `_is_cached`, `_is_parquet_readable` to `XetraService`. Save raw bytes in `fetch_and_parse_trades` before parsing. Replace entire two-tier resume detection block in `fetch_and_store_missing_trades_incremental` with `_is_cached` check. Remove download log write path. Remove quarantine write (raw cache covers it).
- [x] **Step C** — Add `cleanup_raw_cache(venue, max_age_days=7)` method; wire into daemon per-cycle run in `xetra_cli.py`. Add `cleanup-raw-cache` CLI command.
- [x] **Step D** — Add `reprocess_from_raw_cache(venue, date_str, force=False)` method; add `reprocess-raw-cache` CLI command.
- [x] **Step E** — Tests: create `tests/test_xetra_raw_cache.py` (atomic write, non-fatal failure, skip-on-cached, cleanup TTL, reprocess recovery). `TestDownloadLogTracking` replaced with `TestRawCacheTracking` in `tests/test_xetra_service.py`.
- [x] **Step F** — Update `.github/STORAGE_STRUCTURE.md`: add `de/xetra/raw/` layout and TTL semantics, note `.download_log.parquet` deprecated. Update `CLAUDE.md` CLI table.
- [x] **Step G** — Redesign `save_xetra_trades()` as append-only write model (item #1). Implemented as mini-file pattern: `store_trades()` writes `trades-{pid}-{timestamp_ns}.parquet` per call (O(1), no read). `_consolidate_daily_files()` merges them into `trades.parquet` at end of date, with crash-safe cleanup (if `trades.parquet` already exists on restart, mini-files are stale and deleted). `backend.save_xetra_trades()` retained for backward compatibility.
- [x] **Step H** — Change consolidation cadence from per-date to per-month-rollover (item #2). `fetch_and_store_missing_trades_incremental` tracks `last_processed_month`; triggers `_consolidate_to_monthly` when month changes between dates, and after the loop for any completed past month. No per-date monthly consolidation.
- [x] **Step I** — Hygiene cleanup pass (items #3, #9): collapsed dead `if/else` branches in `get_missing_dates` (both branches were identical — now returns `available_dates` directly). Removed inline `import pyarrow`, `import os`, `import shutil` from `_consolidate_to_monthly`; replaced `shutil.move` with `Path.replace`.
- [x] **Step J** — Added httpx client rotation on `RemoteProtocolError`/`ConnectError` in `download_file` retry loop. Added `rotate_client()` method to `XetraFetcher`. Client is recreated on each stale-connection error before retry.
- [x] **Step K** — Migration preflight sentinel: `find_unmigrated_files` writes `.migration_complete` when the scan returns empty (including daemons already migrated before this change was deployed — handled automatically on first post-upgrade scan). `migrate_legacy_columns` also writes it on success. Both check for the sentinel at entry and return `[]` immediately if present.
- [ ] **Step L** — ~~Concurrent download/parse/store pipeline (item #8).~~ **Won't do.** The download path is rate-limited to ~1,870ms/file amortized (0.6s inter-request + 35s burst cooldown every 30 files). Parse+store combined is ~15–65ms — roughly 1–3% of per-file budget. The original 30–40% throughput estimate assumed the O(n²) store path that Step G eliminated; that bottleneck no longer exists. Adding a producer/consumer queue would require thread-safe rate-limiter coordination and error propagation across threads for an immeasurable real-world gain. Revisit only if the rate limit is relaxed or files become substantially larger.
- [ ] **Step M** — ~~Direct PyArrow parser path (item #10).~~ **Won't do.** At ~1,000 rows/file the `pd.DataFrame(trades)` allocation is ~5–15ms against a 1,870ms/file rate-limiter floor — roughly 1% of budget. `store_trades` already calls `pa.Table.from_pandas(df)` downstream; at 1K rows that conversion is negligible. A PyArrow rewrite would require reimplementing ISO 8601 UTC timestamp parsing, type coercion, and column validation in PyArrow compute expressions — substantially more complex code with no measurable runtime benefit at current file sizes. Revisit if files grow to 100K+ rows and parse measurably dominates.

Run `uv run pytest` after each step; all tests must pass before moving on.

## Risk Controls

- Items #1 and #4 change the on-disk write pattern. Existing daily Parquet files must remain readable by all current consumers (incremental resume-check, monthly consolidation, future OHLCV aggregator). Add a regression test that writes a day end-to-end with the new path and reads it back via `pq.read_table` and `pd.read_parquet`.
- The raw cache cleanup (Step C) must not delete raw files until a readable Parquet (daily or monthly) confirms the data is preserved. Cleanup gates on `_is_parquet_readable(daily_path) or _is_parquet_readable(monthly_path)`.
- `reprocess_from_raw_cache` (Step D) must be safe to run against an existing (possibly corrupt) Parquet — `save_xetra_trades` will log a warning on read failure and overwrite, which is the correct behavior.
- Items #2 and #4 alter operational behavior visible in logs (consolidation now monthly, not daily; download log gone). Update the relevant operational docs so on-call doesn't think something has broken.
- Disk space ceiling for raw cache: ~336 MB for 4 venues × 7 days at ~15 KB/file. Document in ops guide.

## Alternatives Considered

**Do nothing — current performance is acceptable.** Rejected: the O(n²) shapes are not visible at today's scale (one venue, one daemon) but will compound when DFRA/DGAT/DEUR are enabled and when months of intra-day data accumulate. Cheaper to fix now than to firefight later.

**Per-minute files + offline daily roll-up.** Considered for item #1. Rejected as primary approach because it inflates file count by ~800× and forces every consumer (incremental resume-check, query layer, future OHLCV) to glob-merge. Append-only `ParquetWriter` preserves the one-file-per-day invariant.

**Switch to DuckDB-backed storage for the intra-day write path.** Considered. Rejected for now: introduces a new runtime dependency in the hot path, and the append-only `ParquetWriter` solution achieves the same goal with the existing PyArrow stack. DuckDB remains the right choice for the query layer (separate ADR).

**Keep the download log, rewrite as append-only ParquetWriter.** Considered for item #4. Rejected in favour of the raw JSON cache: the cache solves data safety (SIGKILL resilience) and resume detection simultaneously, whereas an append-only log only solves resume detection. The cache also handles empty files naturally (their raw bytes are stored regardless of whether they produce any Parquet rows).

## Consequences

- Resume-detection is now O(1) per file (`path.exists()`) and correct for all schemas and all venues.
- Raw JSON files are preserved 7 days, making Parquet corruption or SIGKILL mid-write a recoverable operational event rather than data loss.
- The download log (`.download_log.parquet`) is deprecated and removed from the write path. Existing log files on disk are left in place and will be ignored.
- Daemon write throughput becomes O(1) per file rather than O(n) growing through the day once item #1 is also done — total daily I/O drops by roughly two orders of magnitude.
- Memory footprint becomes proportional to one row group, not the accumulated daily file, once item #1 is done.
- The remaining hygiene items (#3, #9, #10, #11) are low-risk cleanups that can be batched.
