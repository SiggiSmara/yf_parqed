# ADR 2026-10-03: Daemon Resource Footprint (Memory, Disk I/O, Shutdown)

## Status: To-Do

**Deadline for Step A: before 2026-11-01**, when the Xetra problem described below next recurs.

## Context

Both daemons run on one small machine that other projects share: 2 CPU cores, 3.7 GiB RAM, a 3.7 GiB swap file and a single 7200 rpm hard disk. On 2026-10-01 and 2026-10-02 another project on that machine reported that memory kept running out and swap was heavily used. An investigation on 2026-10-03 confirmed it and found three separate problems. None of them lost data.

### 1. Xetra: the monthly consolidation repeats every cycle and does not fit in RAM

System history (`sar`, 10-minute samples) for the days around the month change:

| Day | Peak swap used | Lowest available RAM | Average iowait |
|---|---|---|---|
| Sep 25–28 | 1% (~34 MB) | ~2.9 GiB | 8% |
| Oct 1 | 81% | 21 MB | 24% |
| Oct 2 | 95% | 59 MB | 41% |
| Oct 3 | 18% (leftover, no swap traffic) | 2.2 GiB | 12% |

The Xetra log shows `Month rolled over 2026-09 → 2026-10, consolidating` on every fetch cycle: 18 times on Oct 1 and 13 times on Oct 2. Every run read the same 22 daily files (8,140,348 trades) and rewrote the same 244 MB monthly file. A run took between 6 and 70 minutes, depending on how hard the machine was swapping. The lifetime peaks systemd recorded for `system-xetra.slice` are 3.66 GB of RAM and 3.14 GB of swap, which is the whole machine.

Two things in `XetraService` combine to cause this:

- **The trigger has no memory.** `get_missing_dates` returns every date the API currently lists (a rolling window of about three trading days), not only dates that are missing locally. `fetch_and_store_missing_trades_incremental` calls `_consolidate_to_monthly` whenever two consecutive dates in that list fall in different months, and again after the loop when the last date belongs to a past month. Neither call checks whether the monthly file is already up to date. So for as long as the API window touches the previous month, every cycle consolidates it again.
- **The consolidation holds the whole month in memory several times.** `_consolidate_to_monthly` reads each daily file into a pandas DataFrame, concatenates them, and converts the result to an Arrow table. That is about three copies of 8 million rows with 11 string columns.

This is a gap in [ADR 2026-05-01: Xetra Daemon Write-Path Performance and Hygiene](../implemented/2026-05-01-xetra-daemon-write-path-perf.md). Its Step H moved consolidation from "after every date" to "when the month changes between dates", and its Step I deliberately left `get_missing_dates` returning every available date. Neither step made consolidation idempotent or bounded its memory.

The modification times of the monthly files (Jun 3, Jul 3, Aug 3, Sep 2, Oct 2) show the same thing has happened at the start of every month since May. It stops by itself once the API window contains only the new month.

One smaller defect in the same function: it sorts by a column named `time`, which no longer exists (the column is `trading_date_time` since the May 2026 migration). The sort has silently not run since then, so monthly files are ordered by day and, within a day, by download order.

### 2. Yahoo: every cycle refetches and rewrites everything

The Yahoo daemon is not a memory problem (about 250 MB resident, nothing in swap). It is the constant load: 5 to 6 CPU-hours per day and, while a cycle runs, 20–35% I/O pressure on the shared disk.

- **The ticker registry is never saved by the daemon.** `run_update_once` saves `tickers.json` only when the `--save-not-founds` flag is set, and the systemd unit does not set it. Every cycle ends with `Tickers file was not updated.` The scheduler reloads the registry from disk at the start of each cycle, so everything a cycle learned is discarded. On disk, 5 of 9,276 tickers have any interval metadata.
- **So every ticker looks new every cycle.** With no `last_data_date`, `save_single_stock_data` takes the `load_all` path: it fetches the full 7 days of 1-minute bars for every ticker on every cycle, including weekends.
- **And every partition is rewritten.** `PartitionedStorageBackend.read` loads every monthly partition of the ticker, and `_write_partitions` writes every month back with an fsync, not only the month the new data belongs to. On Saturday 2026-10-03 all 12 of AAPL's monthly files were rewritten at 06:31.
- **Scale:** 93,472 partition files (4.8 GiB) are read and rewritten per cycle. A cycle takes 4h08m, sleeps 2h, and starts again.

A side effect: the not-found bookkeeping (streak, cooling-off, `permanently_dead`) lives in the same unsaved registry, so it cannot advance in the daemon. One cycle on Oct 3 logged 1,758 "possibly delisted" responses from tickers that are asked again every cycle.

### 3. Both daemons are SIGKILLed every night

Logrotate's `postrotate` runs `systemctl reload-or-restart yf-parqed 'xetra@*'` at 00:00 each day. Neither unit has a reload action, so both are restarted.

- **Yahoo:** the SIGTERM handler sets a flag that the loop checks only between cycles. A cycle lasts about four hours, `TimeoutStopSec` is 60 seconds, so systemd kills the process (seen Oct 2 and Oct 3; on Oct 1 it happened to be between cycles and stopped cleanly). The next start finds a stale run lock and scans for leftover temp files before working; on Oct 3 that scan took 13 minutes.
- **Xetra:** when idle it notices the signal within 10 seconds and logs `Daemon shutting down gracefully`, but the process then does not exit and is killed at `TimeoutStopSec=30` (seen Oct 1 and Oct 3). `PID file removed` is never logged. **The cause of this hang has not been identified.** When a cycle is running, the flag is not checked until the cycle ends (Oct 2: the stop arrived during a consolidation and the process was killed).
- The Yahoo daemon logs only to the journal. The rotated files are all Xetra's, so restarting the Yahoo service serves no purpose.

Writes are atomic (temp file, fsync, rename), which is why the nightly kill has not corrupted data. It still costs a restart, a recovery scan, and an immediate extra cycle.

## Decision

Fix the three problems in the order below, then add memory limits as a safety net.

### 1. Make Xetra monthly consolidation idempotent and streaming

- **Skip when current.** `_consolidate_to_monthly` returns early when the monthly file exists and is newer than every daily `trades.parquet` for that month. The existing triggers stay as they are; repeat calls become cheap no-ops.
- **Stream one day at a time.** Replace the pandas read, concat and convert with a PyArrow `ParquetWriter` on the temp file: read one daily table, write it, release it, move to the next. Peak memory becomes one trading day (370,000 to 475,000 rows in the days examined) instead of a month. The temp file, fsync and rename stay.
- **Unify schemas up front.** Read the schemas of the daily files (metadata only), unify them, and cast each daily table to the unified schema before writing, so a schema change within a month does not abort the write.
- **Drop the dead sort.** Remove the `time` sort rather than repair it. A global sort needs the whole month in memory, which is the thing being removed. Monthly files are ordered by day, then by download order within the day. Consumers that need time order sort on `trading_date_time`.

### 2. Make the Yahoo cycle incremental

- **Persist the registry.** In daemon mode, save `tickers.json` at the end of every cycle regardless of `--save-not-founds`, and also every N tickers during a cycle so that a crash loses minutes, not hours. The save must be atomic (temp file and rename).
- **Write only the months the new data touches.** `PartitionedStorageBackend.save` writes only partitions whose month appears in `new_data`. Untouched months are left alone.
- **Then read only those months.** Once writes are scoped, restrict the read to the same months, so a cycle no longer opens every partition of every ticker.

### 3. Make shutdown prompt and stop the nightly restart

- **Check the flag inside the loops.** The Yahoo per-ticker loop and the Xetra per-date and per-file loops check the shutdown flag between items. An atomic write in progress is always allowed to finish.
- **Find and fix the Xetra exit hang.** Establish why the process does not exit after the daemon loop ends.
- **Stop restarting from logrotate.** Use `copytruncate` for the Xetra log file and remove the `postrotate` restart. The templates live in `daemon-manage.sh` and `docs/daemon/INSTALLATION.md`.

### 4. Add systemd memory limits

Add `MemoryHigh`, `MemoryMax` and `MemorySwapMax` to both unit templates so that a future regression is contained to the service that caused it. `.github/ARCHITECTURE.md` already recommends this; the templates never got it. Choose the values from the peaks observed after Steps A to C are deployed.

## Sequenced Steps

- [ ] **Step A** — Xetra consolidation: skip-when-current guard, streaming `ParquetWriter`, schema unification, remove the dead `time` sort. Tests: a second call does not rewrite the file; a month with a changed daily file is re-consolidated; streamed output has the same rows as the daily inputs; mixed-schema days consolidate. **Must be deployed before 2026-11-01.**
- [ ] **Step B** — Yahoo registry persistence: save at end of cycle and every N tickers in daemon mode, atomically. Review the fetch decision first (see Risk Controls). Tests: a second cycle sees `last_data_date`; a simulated kill mid-cycle keeps the last periodic save.
- [ ] **Step C** — Yahoo scoped writes: write only touched months. Test: files of untouched months keep their modification time and content.
- [ ] **Step D** — Yahoo scoped reads: read only the months needed for the merge.
- [ ] **Step E** — In-loop shutdown checks for both daemons. Tests: a flag set mid-cycle ends the cycle after the current item.
- [ ] **Step F** — Diagnose and fix the Xetra exit hang.
- [ ] **Step G** — Logrotate: `copytruncate`, no restart. Update `daemon-manage.sh`, `docs/daemon/INSTALLATION.md` and `docs/DAEMON_MODE.md`.
- [ ] **Step H** — Memory limits in both unit templates (`daemon-manage.sh`, `docs/daemon/INSTALLATION.md`), with values taken from observed peaks. Only after Step A.
- [ ] **Step I** — Update `.github/TROUBLESHOOTING.md`, `docs/release-notes.md` and this ADR's status.

Run `uv run pytest` after each step; all tests must pass before moving on.

## Risk Controls

- **Daily files stay the record.** Consolidation never deletes or modifies daily files. Before deploying Step A, run the new consolidation in dev against a copy of a real month and compare row count and per-ISIN counts with the existing monthly file.
- **Raw-cache cleanup is unchanged.** It still deletes raw files only when a readable daily or monthly Parquet exists.
- **Step B activates a code path production has effectively never run.** Once `last_data_date` is saved, the fetch decision becomes `business_days_between(last_data_date, today) > 0`, which works in whole days. For 1-minute data on a 2-hour cycle this must be reviewed and covered by tests before deploying, so that saving the registry does not leave gaps. Yahoo serves only the last 7 days of 1-minute bars, so a ticker that goes unfetched for longer loses data permanently.
- **Step B also lets the not-found cycle advance.** Tickers will start reaching `permanently_dead` for the first time in the daemon. Check the first week's registry changes against expectations before trusting the pruning that follows.
- **Scoped writes must not drop rows.** A test must show that rows merged into a touched month are complete and that deduplication within that month still works.
- **Memory limits before Step A would do harm.** With the current code a limit makes the monthly consolidation fail every month. Step H depends on Step A.

## Alternatives Considered

**Add RAM or move to an SSD.** Rejected as the fix: it hides the waste without removing it, and the work grows with every month of data and every ticker. Still worthwhile later for other reasons.

**Memory limits only.** Rejected: the consolidation would be killed each month and the monthly file would never be produced.

**Make `get_missing_dates` return only missing dates.** Considered (it is the alternative noted under item #3 of the 2026-05-01 ADR). It would narrow the trigger, but the current day must be rechecked every cycle anyway, and it does not make consolidation safe to call twice or bound its memory. It can be done in addition; it is not a substitute.

**Polars or DuckDB for the consolidation.** Both can stream. Rejected for now because PyArrow is already in the write path and one day at a time is enough; no new dependency in the daemon's hot path.

**Append each day to the monthly file as it completes.** Considered. Rejected for now: Parquet files cannot be appended in place, so this means rewriting the monthly file daily or keeping a writer open across restarts. Once-per-month streaming is simpler and cheap.

## Consequences

- Xetra consolidation runs once per month, in memory proportional to one trading day, and repeated triggers cost a few `stat` calls.
- A Yahoo cycle touches only the current month's partition per ticker and skips tickers with nothing new, which removes most of the disk writes and should shorten the 4-hour cycle substantially. The actual gain is to be measured after Steps B to D.
- The daemons stop within seconds of a stop request and are no longer restarted nightly.
- Monthly Xetra files are explicitly not globally time-sorted. This matches what has been on disk since May 2026.
- A runaway daemon is contained by its own memory limit instead of pushing the whole machine into swap.
