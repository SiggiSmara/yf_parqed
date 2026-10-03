# ADR 2026-10-03: Daemon Resource Footprint (Memory, Disk I/O, Shutdown)

## Status: In Progress

**Deadline for Step A: it must be deployed before 2026-11-01**, when the Xetra problem described below next recurs.

**Picking this up in a new session?** Read "Work Plan and Handoff" below first. It says which batch is next, what is already done, and how to check a deploy.

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

- **Skip when current.** The monthly file records a fingerprint of the daily files it was built from (their count, total size and newest modification time) in its Parquet metadata. `_consolidate_to_monthly` returns early when that fingerprint matches the daily files on disk. The existing triggers stay as they are; repeat calls cost a few `stat` calls and one footer read. Monthly files written before this change carry no fingerprint and are rebuilt once if their month is triggered again.
- **An unreadable daily file aborts the run.** The previous code skipped such a day and wrote a monthly file that silently lacked it. Now the consolidation fails, the existing monthly file is left as it is, and the error is logged every cycle until the daily file is repaired (for example with `reprocess-raw-cache`).
- **Stream one day at a time.** Replace the pandas read, concat and convert with a PyArrow `ParquetWriter` on the temp file: read one daily table, write it, release it, move to the next. Peak memory becomes one trading day (370,000 to 475,000 rows in the days examined) instead of a month. The temp file, fsync and rename stay.
- **Unify schemas up front.** Read the schemas of the daily files (metadata only), unify them, and cast each daily table to the unified schema before writing, so a schema change within a month does not abort the write.
- **Drop the dead sort.** Remove the `time` sort rather than repair it. A global sort needs the whole month in memory, which is the thing being removed. Monthly files are ordered by day, then by download order within the day. Consumers that need time order sort on `trading_date_time`.

### 2. Make the Yahoo cycle incremental

The order matters. The first two changes cut the disk load without changing what is fetched, so the current "fetch 7 days every cycle" behaviour keeps protecting the data while they settle. The third is the only one that changes fetch behaviour, so it lands last and alone.

- **Write only the months the new data touches.** `PartitionedStorageBackend.save` writes only partitions whose month appears in `new_data`. Untouched months are left alone. With a 7-day fetch that is one month per ticker, two around a month boundary, instead of all of them.
- **Then read only those months.** Once writes are scoped, restrict the read to the same months, so a cycle no longer opens every partition of every ticker.
- **Finally, persist the registry.** In daemon mode, save `tickers.json` at the end of every cycle regardless of `--save-not-founds`, and also every N tickers during a cycle so that a crash loses minutes, not hours. The save must be atomic (temp file and rename).

### 3. Make shutdown prompt and stop the nightly restart

- **Check the flag inside the loops.** The Yahoo per-ticker loop and the Xetra per-date and per-file loops check the shutdown flag between items. An atomic write in progress is always allowed to finish.
- **Find and fix the Xetra exit hang.** Establish why the process does not exit after the daemon loop ends.
- **Stop restarting from logrotate.** Use `copytruncate` for the Xetra log file and remove the `postrotate` restart. The templates live in `daemon-manage.sh` and `docs/daemon/INSTALLATION.md`.

### 4. Add systemd memory limits

Add `MemoryHigh`, `MemoryMax` and `MemorySwapMax` to both unit templates so that a future regression is contained to the service that caused it. `.github/ARCHITECTURE.md` already recommends this; the templates never got it. Choose the values from the peaks observed after Batches 1 to 3 are deployed.

## Sequenced Steps

Steps are grouped into batches. A batch is one working session, one commit series and one deploy. Batches run in order, one at a time; see "Work Plan and Handoff" for why and for who does what.

**Batch 1 — Xetra consolidation**

- [ ] **Step A** — Skip-when-current fingerprint, streaming `ParquetWriter`, schema unification, abort on an unreadable daily file, remove the dead `time` sort. Tests: a second call does not rewrite the file; a month with a new daily file is re-consolidated; streamed output equals the daily inputs; mixed-schema days consolidate; an unreadable daily file leaves the existing monthly file untouched. **Must be deployed before 2026-11-01.**

**Batch 2 — Shutdown and logrotate**

- [ ] **Step E** — In-loop shutdown checks for both daemons. Tests: a flag set mid-cycle ends the cycle after the current item.
- [ ] **Step F** — Diagnose and fix the Xetra exit hang.
- [ ] **Step G** — Logrotate: `copytruncate`, no restart. Update `daemon-manage.sh`, `docs/daemon/INSTALLATION.md` and `docs/DAEMON_MODE.md`.

**Batch 3 — Yahoo disk load and registry** (two deploys: B and C together, then D alone)

- [ ] **Step B** — Scoped writes: write only touched months. Tests: files of untouched months keep their modification time and content; rows merged into a touched month are complete and deduplicated.
- [ ] **Step C** — Scoped reads: read only the months needed for the merge.
- [ ] **Step D** — Registry persistence: save at end of cycle and every N tickers in daemon mode, atomically. Review the fetch decision first (see Risk Controls). Tests: a second cycle sees `last_data_date`; a simulated kill mid-cycle keeps the last periodic save. Deploy separately from B and C, and only after they have run cleanly for a few days.

**Batch 4 — Memory limits**

- [ ] **Step H** — Memory limits in both unit templates (`daemon-manage.sh`, `docs/daemon/INSTALLATION.md`), with values taken from observed peaks. Only after Batches 1 to 3 have been deployed and observed for about a week.

**With every batch**

- [ ] **Step I** — Keep `.github/TROUBLESHOOTING.md`, `docs/DAEMON_MODE.md`, `docs/release-notes.md` and this ADR in step with what was changed. When the last batch is done, move this ADR to `implemented/` and update the index.

Run `uv run pytest` after each step; all tests must pass before moving on.

## Work Plan and Handoff

### How the work is split

| Batch | Steps | Session | Model | Why |
|---|---|---|---|---|
| 1 | A | The session that wrote this ADR (2026-10-03) | Opus | Smallest scope, hard deadline, and the context was already loaded. |
| 2 | E, F, G | New session | Opus for F; Sonnet is enough for E and G | F is an open diagnosis. E and G are mechanical and fully specified here. |
| 3 | B, C, then D | New session | Opus | Storage and fetch logic where a mistake loses data permanently. |
| 4 | H | Any, about a week after Batch 3 | Sonnet | A few lines in two templates; the work is choosing the values. |

**Do not run batches in parallel.** Development happens on the production host (2 cores, 3.7 GiB RAM), so several agents running the test suite compete with the daemons. The shutdown work (E) also touches the same files as Steps A and D. And each batch changes production behaviour that should be watched before the next one lands.

### Starting a session

1. Read this ADR. The Context section holds the evidence; it cannot be regenerated, because `sar` keeps only about a week.
2. Find the lowest batch with an unchecked step. Read the Progress Log below for anything the previous session left open.
3. Read the Risk Controls that mention your steps before writing code.
4. Work the steps in order. Tick each box when its tests pass. Add a dated line to the Progress Log when you stop, including anything unfinished or surprising.

### Deploying

The agent cannot deploy. Each batch ends with commits on `develop`; the owner then runs `sudo ./daemon-manage.sh update`, which pulls the code and restarts the services. Record the deploy date in the Progress Log.

### Checking a deploy

- **Batch 1.** The proof arrives at the next month change (2026-11-01 to 11-04). In `/var/log/yf_parqed/xetra-DETR.log*`, `Consolidated to monthly` should appear once for October, and `sar -S` should show swap staying near its baseline. Before that, the dev validation in the Progress Log is the evidence.
- **Batch 2.** After the next midnight, `journalctl -u yf-parqed -u 'xetra@DETR' | grep -E 'timed out|SIGKILL'` should show nothing new, and neither service should have restarted at 00:00.
- **Batch 3, after B and C.** Pick a ticker and list its partition files: only the current month's file should have a fresh modification time after a cycle. Cycle duration (from `Processing ... tickers` to `All tickers were processed.` in `journalctl -u yf-parqed`) should fall well below the 4h08m baseline.
- **Batch 3, after D.** For a week, repeat the gap check below and compare with the baseline. Also confirm that `tickers.json` now carries `last_data_date` values and that the list of tickers going `permanently_dead` looks reasonable.

### Baseline for the Yahoo gap check (measured 2026-10-03)

Count 1-minute bars per trading day for a few liquid tickers by reading the `date` column of `/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=<T>/`.

- AAPL, MSFT, NVDA, JPM, XOM and ZTS each had 214 trading days from 2025-11-25 to 2026-10-02 with a median of 390 bars per day (a full session).
- No business day was missing except market holidays. Half-day sessions (2025-11-28, 2025-12-24) have 196 to 210 bars.
- One known anomaly: 2026-01-06 has only 293 to 323 bars for all six. Cause unknown; it predates this work.
- Thinly traded instruments (SPAC units and the like) legitimately have gaps of weeks. Use liquid tickers for the check.

After Step D, any liquid ticker with a missing business day or a day well short of 390 bars is a regression. Yahoo serves only the last 7 days of 1-minute data, so act within that window: revert Step D and let the old fetch-everything behaviour refill the gap.

### Progress Log

- **2026-10-03** — Investigation done, ADR written, steps ordered into batches. Step A started in the same session.

## Risk Controls

- **Daily files stay the record.** Consolidation never deletes or modifies daily files. Before deploying Step A, run the new consolidation in dev against a copy of a real month and compare row count and per-ISIN counts with the existing monthly file.
- **Raw-cache cleanup is unchanged.** It still deletes raw files only when a readable daily or monthly Parquet exists.
- **Step D activates a code path production has effectively never run.** Once `last_data_date` is saved, the fetch decision becomes `business_days_between(last_data_date, today) > 0`, which works in whole days. For 1-minute data on a 2-hour cycle this must be reviewed and covered by tests before deploying, so that saving the registry does not leave gaps. Yahoo serves only the last 7 days of 1-minute bars, so a ticker that goes unfetched for longer loses data permanently.
- **Step D also lets the not-found cycle advance.** Tickers will start reaching `permanently_dead` for the first time in the daemon. Check the first week's registry changes against expectations before trusting the pruning that follows.
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
