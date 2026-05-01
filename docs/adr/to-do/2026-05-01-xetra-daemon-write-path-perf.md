# ADR 2026-05-01: Xetra Daemon Write-Path Performance and Hygiene

## Status: To-Do

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

### 2. `_consolidate_to_monthly()` — run once per month, not after every successful date

**File:** `src/yf_parqed/xetra/xetra_service.py` (`_consolidate_to_monthly`, lines 687–804; called from lines 642–654)

After every completed date, the service re-reads **all** daily files for the month, concatenates, and rewrites the monthly file. On the 22nd day of a month that re-loads 21 already-consolidated days. Same O(n²) shape as item #1, at month scale.

**Recommended fix:** trigger consolidation once per month (when the month rolls over, or on first daemon iteration of a new month), not after each successful date. Alternative: append the new day's row group to the existing monthly file via `ParquetWriter` rather than reading-and-rewriting.

### 3. `get_missing_dates()` — dead branching, always returns every available date

**File:** `src/yf_parqed/xetra/xetra_service.py` (`get_missing_dates`, lines 155–187)

The if/else block conditions on whether the daily Parquet exists, but both branches `append`. The "incremental logic will check which files are already stored" comment is honest but the differentiation is meaningless. Cheap cleanup; collapse to an unconditional append, or actually use the existence check to skip dates that don't need re-checking.

### 4. Download log — O(n²) rewrite every 10 files

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 582–619)

`pd.read_parquet → concat → drop_duplicates → to_parquet` on the entire centralized download log, every 10 files, for every venue, for every day. After a few months of operation this is millions of rows being rewritten dozens of times per minute.

**Recommended fix:** drop the centralized log entirely. The fallback path (lines 471–508) already infers "downloaded" from distinct minute-level timestamps in the trades Parquet — make that the only path. If we keep a log for empty files, use an append-only `ParquetWriter` per (venue, day) instead of read-merge-rewrite.

### 5. `trade_time` column reference — silent regression after legacy migration

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 476–478)

```python
df_existing = pd.read_parquet(parquet_path, columns=["trade_time"])
```

After the 2025-legacy → MiFIR column migration, the column is `trading_date_time`. This raises on every migrated file. The surrounding `try/except` swallows the error silently, so resume-detection falls back to the (also-broken) download log, and the daemon ends up re-downloading files it already has.

**Recommended fix:** rename the reference to `trading_date_time`. During a transition period, attempt both names and use whichever exists.

### 6. `XetraFetcher.client` — single `httpx.Client` for the lifetime of the daemon

**File:** `src/yf_parqed/xetra/xetra_fetcher.py` (line 73)

httpx connections can go stale on long-running daemons (TCP keepalive, proxy timeouts, DNS rotation). No reported failures yet, but worth either:
- Periodically rotating the client at a low-traffic boundary (e.g., between daemon iterations)
- Adding retry-on-`RemoteProtocolError`/`ConnectError` in the download path

### 7. Hard-coded `"DETR-posttrade-"` filename parsing

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 515, 564)

```python
filename.split("DETR-posttrade-")[1]
```

Silently breaks for DFRA/DGAT/DEUR — the other venues the fetcher already supports. Today the daemon only runs DETR, so this is dormant. Use the `venue` parameter that's already in scope: `f"{venue}-posttrade-"`.

### 8. Sequential download/parse/store — missed concurrency opportunity

**File:** `src/yf_parqed/xetra/xetra_service.py` (line 539)

The per-file loop is strictly serial: download → parse → store → next download. Download is rate-limited and sleep-bound (~0.6s between requests + 35s burst cooldown every 30 files). Parse and store are CPU/I/O bound. Running parse+store concurrently with the next download via a small queue would yield an estimated 30–40% throughput gain on catch-up runs, with no impact on steady-state.

Worth doing only if catch-up speed matters. Given we just had a 40-day outage where catch-up speed mattered a lot, the answer is probably yes — but lower priority than items 1, 2, 4.

### 9. `import` statements inside hot loops

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 449, 473, 586; `_consolidate_to_monthly` at 713, 781–783)

`import pandas as pd` and similar appear inside per-file loops. Functionally harmless (Python caches), but signals copy-paste accumulation. Move to module top.

### 10. `XetraParser.parse()` — pandas detour for small JSONL files

**File:** `src/yf_parqed/xetra/xetra_parser.py` (lines 108–109)

`pd.DataFrame(trades)` followed by rename works fine for ~1000-row files but is a needless pandas allocation. Building a PyArrow table directly via `pa.Table.from_pylist(...)` would be cheaper and align the parser output type with the storage layer's preferred input. Low priority unless file sizes grow significantly.

### 11. `find_unmigrated_files()` — runs `pq.read_schema()` on every file on every daemon iteration

**File:** `src/yf_parqed/xetra/xetra_service.py` (lines 1037–1063)

The migration preflight scans the entire venue tree on every daemon startup. Once a file is migrated it stays migrated; there's no need to re-check. Add a sentinel file (`.migration_complete`) or persist the result, and skip the scan when it's set.

## Sequenced Steps

Recommended priority order:

- [ ] **Step 1** — Redesign and implement `save_xetra_trades()` as an append-only `ParquetWriter` model (item #1). This is the largest single perf win and addresses the original concern that motivated the review.
- [ ] **Step 2** — Change consolidation cadence from per-date to per-month-rollover (item #2).
- [ ] **Step 3** — Fix the `trade_time` → `trading_date_time` reference (item #5). Silent correctness regression — fix soon regardless of the perf work.
- [ ] **Step 4** — Decide the future of the centralized download log: delete or rewrite as append-only (item #4). Pairs naturally with Step 1.
- [ ] **Step 5** — Hygiene cleanup pass (items #3, #7, #9): collapse dead branching, replace hard-coded venue prefix, hoist imports.
- [ ] **Step 6** — Add httpx client rotation/retry-on-stale-connection (item #6).
- [ ] **Step 7** — Cache the migration preflight result (item #11).
- [ ] **Step 8** — *(Optional)* Concurrent download/parse/store pipeline (item #8). Only if catch-up throughput becomes a recurring need.
- [ ] **Step 9** — *(Optional)* Direct PyArrow parser path (item #10). Only if file sizes grow.
- [ ] **Step 10** — Run `uv run pytest` after each step; all tests must pass before moving on.

## Risk Controls

- Items #1 and #4 change the on-disk write pattern. Existing daily Parquet files must remain readable by all current consumers (incremental resume-check, monthly consolidation, future OHLCV aggregator). Add a regression test that writes a day end-to-end with the new path and reads it back via `pq.read_table` and `pd.read_parquet`.
- Item #1 must handle daemon restart mid-day without losing already-written rows or producing unreadable files. The existing tmp-file-then-rename pattern needs an analogue for the append-only writer (e.g., write to `trades.parquet.partial`, rename on writer close, and on startup either reopen or roll the partial into the final file).
- Item #5 changes resume-detection — verify with a test that creates a migrated file, deletes some minutes from the download log, and confirms only those minutes are re-fetched.
- Items #2 and #4 alter operational behavior visible in logs (consolidation now monthly, not daily; download log gone or smaller). Update the relevant operational docs (`.github/STORAGE_STRUCTURE.md` if applicable) so on-call doesn't think something has broken.

## Alternatives Considered

**Do nothing — current performance is acceptable.** Rejected: the O(n²) shapes are not visible at today's scale (one venue, one daemon) but will compound when DFRA/DGAT/DEUR are enabled and when months of intra-day data accumulate. Cheaper to fix now than to firefight later.

**Per-minute files + offline daily roll-up.** Considered for item #1. Rejected as primary approach because it inflates file count by ~800× and forces every consumer (incremental resume-check, query layer, future OHLCV) to glob-merge. Append-only `ParquetWriter` preserves the one-file-per-day invariant.

**Switch to DuckDB-backed storage for the intra-day write path.** Considered. Rejected for now: introduces a new runtime dependency in the hot path, and the append-only `ParquetWriter` solution achieves the same goal with the existing PyArrow stack. DuckDB remains the right choice for the query layer (separate ADR).

## Consequences

- Daemon write throughput becomes O(1) per file rather than O(n) growing through the day — total daily I/O drops by roughly two orders of magnitude.
- Memory footprint becomes proportional to one row group, not the accumulated daily file. Production stays well within tight memory bounds even on the AVX2-less server.
- Resume-detection becomes correct again on migrated files (item #5).
- The download log either disappears (preferred) or stops being a hot rewrite target.
- Catch-up runs after outages get faster if step 8 is also done.
- Some backwards-incompatibility risk on the daily Parquet write pattern — mitigated by the regression test in Risk Controls.
- The remaining hygiene items (#3, #7, #9, #10, #11) are low-risk cleanups that can be batched into a single PR.
