# ADR 2026-10-03: Daemon Resource Footprint (Memory, Disk I/O, Shutdown)

## Status: In Progress

**Batch 1 is deployed (2026-10-03), ahead of its 2026-11-01 deadline.** The first real test is the month change on 2026-11-01 to 11-04; see "Checking a deploy". **Batch 2 (Steps E, F, G) is deployed (2026-10-03, 12:32 UTC)**; its first real test is the midnight stop at 00:00 on 2026-10-04. **Batch 3 (Yahoo disk load and registry) is in progress:** Steps B, C and J were deployed on 2026-10-04 and confirmed in production on 2026-10-10. Step D was designed with the owner on 2026-10-10 (Decision 6) and is not started; it deploys alone.

**Picking this up in a new session?** Read "Work Plan and Handoff" below first; its subsection "Next session starts here" names the next piece of work. It also says what is already done and how to check a deploy.

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
- **Xetra:** when idle it notices the signal within 10 seconds and logs `Daemon shutting down gracefully`, but the process then does not exit and is killed at `TimeoutStopSec=30` (seen Oct 1 and Oct 3). `PID file removed` is never logged. The cause was found on 2026-10-03 and is described under "What the exit hang turned out to be" below. When a cycle is running, the flag is not checked until the cycle ends (Oct 2: the stop arrived during a consolidation and the process was killed).
- The Yahoo daemon logs only to the journal. The rotated files are all Xetra's, so restarting the Yahoo service serves no purpose.

Writes are atomic (temp file, fsync, rename), which is why the nightly kill has not corrupted data. It still costs a restart, a recovery scan, and an immediate extra cycle.

#### What the exit hang turned out to be (Step F, 2026-10-03)

It is not a deadlock. The process is exiting the whole time; it is slow because its memory is in swap.

The journal and the Xetra log together cover 37 stops between 2026-09-04 and 2026-10-03:

| What happened | Stops | Belongs to |
|---|---|---|
| Exited 0 to 5 seconds after `Daemon shutting down gracefully` | 20 | nothing to fix |
| Killed because the stop arrived during a cycle (no graceful line) | 14 | Step E |
| Slow after the graceful line: killed at 20 s (Oct 3 00:00) and 25 s (Oct 1 00:00), exited by itself after 21 s (Oct 3 08:09) | 3 | Step F |

A fourth slow exit, a restart on Sep 30 at 06:38, took 13 seconds. So the "hang" is an exit that takes anywhere from 13 to more than 25 seconds, and the 30-second stop timeout (of which the idle loop uses up to 10) is sometimes not enough.

- **Every slow exit coincides with swap traffic.** The 10-minute `sar` sample containing each slow exit shows pages being read back from swap and major page faults: 13 to 17 pages per second for three of them, 930 for Oct 1, whose sample also contains the start of the first consolidation. The samples containing the fast exits show none.
- **The mechanism.** While the Xetra daemon sleeps between cycles, the Yahoo cycle or a consolidation fills RAM and the kernel moves the idle daemon's memory to swap. When the daemon then exits, Python's interpreter teardown frees every object one by one, and to free an object it has to read it back from the hard disk first.
- **Reproduced in dev.** A harness ran the real `fetch-trades --daemon` entry point with the network and storage calls replaced, inside its own cgroup. Writing to the cgroup's `memory.reclaim` pushed the process's 159 MB to swap, as memory pressure does in production; then it was sent SIGTERM. Two runs each:

  | | Loop ended → process gone | Major page faults |
  |---|---|---|
  | Not swapped, old exit | 0.2 s | 0 |
  | Swapped, old exit | 15.2 s and 16.2 s | 11,004 and 11,890 |
  | Swapped, new exit (see Decision 3) | 1.0 s and 0.9 s | 945 and 919 |

  The disk was otherwise idle during these runs. In production the Yahoo cycle uses the same disk, which is why the same exit took longer there.
- **The Yahoo daemon has the same lag.** On Oct 1 it logged `PID file removed` at 00:00:11.4 and the process ended at 00:00:18.9.

**The missing `PID file removed` line has a different cause.** Both unit files declare `RuntimeDirectory=yf_parqed`, and systemd deletes that directory whenever either service stops. On a typical night the Yahoo service was killed at its 60-second timeout, after Xetra had already restarted and written its PID file, so the PID file was deleted with the directory. At the next stop the Xetra daemon found no PID file and logged nothing. The line does appear on the 8 of 37 stops where the Yahoo service had not stopped since the Xetra daemon wrote its PID file. The same deletion made both services fail at start on Oct 1 at 00:00:34 with `Read-only file system: '/run/yf_parqed'`; `Restart=on-failure` brought them back 30 seconds later.

Two more things were seen and belong to Step E:

- Most nightly kills (14 of the 16 in this period) are stops that arrive during a cycle. The hourly cycle drifts until it runs at midnight, and at 00:00 it is usually in the fetcher's 35-second burst cooldown (`XetraFetcher.enforce_limits`), a single `time.sleep` that cannot be interrupted.
- Both daemons log from inside their signal handlers. Loguru's handlers are not re-entrant, so when the signal arrives while the main thread is writing a log line, loguru prints `Could not acquire internal lock ... (deadlock avoided)` to the journal and drops the message (seen Oct 3 at 08:28 and 09:54). It is noise, not the hang.

## Decision

Fix the three problems in the order below, then add memory limits as a safety net.

### 1. Make Xetra monthly consolidation idempotent and streaming

- **Skip when current.** The monthly file records a fingerprint of the daily files it was built from (their count, total size and newest modification time) in its Parquet metadata. `_consolidate_to_monthly` returns early when that fingerprint matches the daily files on disk. The existing triggers stay as they are; repeat calls cost a few `stat` calls and one footer read. Monthly files written before this change carry no fingerprint and are rebuilt once if their month is triggered again.
- **Merge leftover mini-files first.** Before a month is consolidated, any day that still has mini-files is merged into its daily file. Two days in production (2026-05-08, 2026-06-10) had been left as mini-files and were silently missing from their monthly files.
- **Tell stale mini-files from new ones.** The daily merge used to delete every mini-file found next to an existing daily file, on the assumption that they were leftovers of a crash. A daily file now records the staging time of the newest mini-file it contains. Mini-files staged up to that time are deleted as before; later ones are merged in. Daily files written before this change fall back to their own write time. If the daily file cannot be read, the mini-files are kept.
- **Leave an unreadable day out, on the record.** The monthly file is built from the days that can be read and lists those days in its Parquet metadata (`yf_parqed.days_included`). The skipped day is logged as an error. Repairing the daily file changes the fingerprint, so the monthly file is rebuilt at the next trigger.
- **Never shrink a monthly file.** If the readable daily files hold fewer trades than the existing monthly file, the monthly file is kept and an error is logged. This protects months whose daily files were removed or damaged after consolidation. To force a rebuild, remove the monthly file.
- **Integer contract for `price_notation`.** The column is always a nullable integer. A missing value is a null. A value that is not a whole number is stored as null, logged as an error, and the original file is kept under `contract_violations/`. It is not rounded: the field is a code (MiFIR RTS 1 defines `MONE`, `PERC`, `YIEL`, `BAPO`; Deutsche Börse sends a number, so far always `1`), and rounding would relabel the price's unit. One mini-file with a missing value used to turn the column into a float, which blocked the merge of 2026-06-10 for four months.
- **Raw-cache cleanup checks the specific day.** A raw file is deleted only if its day has a readable daily file or is listed in the monthly file's `days_included`. Before, any readable monthly file vouched for every day of its month, which is how the raw files of the two unmerged days were deleted. A monthly file without the list (written before this change) vouches for nothing.
- **Stream one day at a time.** Replace the pandas read, concat and convert with a PyArrow `ParquetWriter` on the temp file: read one daily table, write it, release it, move to the next. Peak memory becomes one trading day (370,000 to 475,000 rows in the days examined) instead of a month. The temp file, fsync and rename stay.
- **Unify schemas up front.** Read the schemas of the daily files (metadata only), unify them, and cast each daily table to the unified schema before writing, so a schema change within a month does not abort the write.
- **Drop the dead sort.** Remove the `time` sort rather than repair it. A global sort needs the whole month in memory, which is the thing being removed. Monthly files are ordered by day, then by download order within the day. Consumers that need time order sort on `trading_date_time`.

### 2. Make the Yahoo cycle incremental

The order matters. The first two changes cut the disk load without changing what is fetched, so the current "fetch 7 days every cycle" behaviour keeps protecting the data while they settle. The third is the only one that changes fetch behaviour, so it lands last and alone.

- **Write only the months the new data touches, and only if they change.** The partitioned backend gets a write path of its own, `PartitionedStorageBackend.merge(request, new_data)`: for each month that has a row in `new_data` it reads that month's file, merges, and writes the file back only when the merged rows differ from the stored ones. With a 7-day fetch that is one month per ticker, two around a month boundary, instead of all of them, and no write at all when every fetched bar is already stored (nights, weekends).
- **Read only those months, inside the backend.** `merge` reads the stored rows itself, from disk, so a caller cannot hand it an incomplete picture of a month. A cycle no longer opens every partition of every ticker, and opens nothing for a ticker whose fetch returns nothing.
- **Captured data is not replaced by less.** Yahoo serves 1-minute bars for 7 days only, so a stored bar that is overwritten by a worse one is gone. `merge` has two guards. An incoming row that holds fewer values than the stored row for the same date is ignored (a bar that comes back without prices does not replace the captured bar); the other incoming rows are still stored. And a month is not written if any stored date would be missing from the merged result; the file is kept and an error is logged. By construction the second cannot happen (the merge is a union by date); it is there for the unforeseen case. Neither guard can judge values that are present but wrong.
- **The legacy layout gets none of this.** The single-file-per-ticker backend and the `save(request, new_data, existing_data)` interface are left as they were; `YFParqed.merge_yf` picks the path. Production has no legacy data. Removing the legacy backend and the migration service is a separate decision, not part of this ADR.
- **Finally, persist the registry.** In daemon mode, save `tickers.json` at the end of every cycle regardless of `--save-not-founds`, and also every N tickers during a cycle so that a crash loses minutes, not hours. The save must be atomic (temp file and rename). What is fetched once the registry is saved was decided on 2026-10-10 and is in Decision 6.

### 3. Make shutdown prompt and stop the nightly restart

- **Check the flag inside the loops.** The Yahoo per-ticker loop and the Xetra per-date and per-file loops check the shutdown flag between items. An atomic write in progress is always allowed to finish.
- **End the daemon process without interpreter teardown.** When a daemon loop has ended and its own cleanup is done (HTTP client closed, run lock released, PID file removed), the process writes out its queued log messages, closes its log files and ends with `os._exit`. The kernel then frees the memory without reading it back from swap. The helper is `exit_daemon_process` in `common/process_exit.py`; the Xetra trade daemon, the ISIN mapping daemon and the Yahoo daemon all use it. One-shot commands and error exits are unchanged.
- **Keep the shared runtime directory.** Both unit templates set `RuntimeDirectoryPreserve=yes`, so stopping one service no longer deletes `/run/yf_parqed` and the other service's PID file with it.
- **Check for a stop request every 10 seconds while idle.** A signal does not cut `time.sleep` short, so a daemon notices a stop only when its current sleep ends. All idle waits (between cycles, outside active hours, the ISIN daemon's wait until 00:05) now sleep in 10-second pieces. The outside-hours waits and the ISIN daemon used 60-second pieces before.
- **Stop restarting from logrotate.** Use `copytruncate` for the Xetra log file and remove the `postrotate` restart. The templates live in `daemon-manage.sh` and `docs/daemon/INSTALLATION.md`.

### 4. Add systemd memory limits

Add `MemoryHigh`, `MemoryMax` and `MemorySwapMax` to both unit templates so that a future regression is contained to the service that caused it. `.github/ARCHITECTURE.md` already recommends this; the templates never got it. Choose the values from the peaks observed after Batches 1 to 3 are deployed.

### 5. Find, record and keep damaged Yahoo files

Decided on 2026-10-03, after Steps B and C removed the full-ticker read that used to notice a damaged file as a side effect. The reasoning: the capture is ephemeral (Yahoo serves 1-minute bars for 7 days), and there is no backup of `/var/lib/yf_parqed/data`. A damaged file can therefore rarely be repaired, so the aims are to never make it worse, to know about it, and to keep what is left of it.

- **Move aside, never delete.** `safe_read_parquet` deletes a file it cannot read and treats every `OSError` as "corrupt", so a passing disk error can delete a month of a ticker's bars. It now reads the file a second time, and if that fails too, renames it (`data.parquet.damaged-<UTC timestamp>`, same directory) and records it. Capture continues into a new file; the old one stays for salvage, and a rename back undoes a false alarm.
- **Record every damaged file.** One line per file in `damaged_partitions.jsonl` in the working directory (when, path, ticker, interval, month, the file's size and modification time, error, what was done, where it was moved, who found it), and an error in the log. A file that a check finds again, unchanged, is not written a second time.
- **Check the last closed month once.** After a cycle that ran to its end (not stopped, not ended by an error), when the last closed month has no entry in the state file, the daemon reads every ticker's file of that month completely, one file at a time, read-only, and records what it finds. A file counts as damaged when it cannot be decoded or holds no rows. The check leaves the file where it is; only the write path renames. A small state file (`partition_checks.json`) says which months were checked and with what result (time, files, rows, damaged), so the check runs once per month; a check cut short by a stop request writes no entry and starts again in a later cycle. Older months are not checked automatically: they cannot be refetched, and a full scan costs about an hour of disk.
- **Verify a new file before it replaces the old one.** After the temp file is written and before the rename, its footer is read back and its row count compared with what was meant to be written. A bad write never replaces a good file. In the same spirit, a temp file left behind by a killed run becomes `data.parquet` only if it reads back completely; otherwise it is kept under a `.damaged-` name.
- **A command for everything else.** `yf-parqed verify-partitions [--month YYYY-MM | --all]` runs the same check on demand; without an option it checks the last closed month. It only reads, so it can run next to the daemon, and it exits with code 1 when it finds a damaged file. The running month is checked by `--all` but gets no entry in the state file, so its own check still happens after it closes.

### 6. Step D: save the registry and fetch once a night

Decided with the owner on 2026-10-10. The data is collected for historical analysis, so how soon a bar is stored does not matter. What matters is that every bar is requested several times before Yahoo drops it, and the 7-day fetch gives that with one fetch per day.

- **One fetch per ticker per night.** A collection night starts at 22:00 UTC. That is after the US close all year (20:00 UTC in summer, 21:00 UTC in winter) and after half-day sessions, so no market calendar and no time-zone conversion is involved. A ticker is fetched when its last successful fetch is older than the most recent 22:00 UTC. Each bar is then requested on 7 nights, where it was requested about 30 times before.
- **Always the last 7 days.** The fetch for 1-minute data stays "the last 7 days" (`_fetch_all`). The window logic based on `last_data_date` is not used.
- **Weekends too.** Saturday and Sunday nights run like any other. They cost two cycles a week, give Friday's bars two more requests, and keep calendar logic out.
- **Later cycles of a night only retry.** The daemon keeps waking every 2 hours. Tickers already fetched that night are skipped, so a later cycle asks only for the tickers whose request failed. The idle wait ends at 22:00 UTC, so that the night's first cycle starts on time.
- **A failed request is not "no data".** yfinance is asked to raise errors (`raise_errors=True`). An answer of "no price data" is a result and ends the ticker's night. A rate limit, a network error or any other exception is a failure: the ticker's saved state is left as it was and it is asked again in the next cycle.
- **A failing ticker does not end the cycle.** The error is logged, the cycle goes on, and a summary line at the end names the failed tickers. Twenty failures in a row end the cycle, because that many point at the host (disk full, permissions, no network) and not at the tickers. A cycle with failed tickers still counts as having run to its end for the month-close check.
- **No ticker is paused or pruned automatically.** Every ticker is asked every night, whether or not it returned data. The not-found streak, the 7-business-day pause and the automatic `permanently_dead` are no longer set. Flags set by `remove-ticker` or `tools/prune_registry.py` are still honoured. Who leaves the registry, and when, is decided in [ADR 2026-10-10: Yahoo Ticker Universe from the Exchange Lists](../to-do/2026-10-10-yahoo-ticker-universe-from-exchange-lists.md).
- **Save the registry during and after a cycle.** Every 500 tickers (about every 11 minutes) and at the end of each cycle, in daemon mode, regardless of `--save-not-founds`. The save writes a temp file, syncs it to disk and renames it. An empty registry is never saved: an unreadable `tickers.json` loads as empty, and saving that would replace the file with nothing.
- **Writers do not overwrite each other.** The daemon saves only the entries of the tickers it processed since its last save, onto a fresh read of the file, while holding a lock file. `add-ticker`, `remove-ticker`, `tools/prune_registry.py` and every other command that writes `tickers.json` take the same lock.
- **The previous release must still fetch everything.** The time of the last successful fetch and the date of the newest stored bar are saved under new keys. `last_data_date` is not written. The previous release switches to its once-per-business-day logic when it finds `last_data_date`, so writing it would turn a revert into a different change in fetch behaviour.

## Sequenced Steps

Steps are grouped into batches. A batch is one working session, one commit series and one deploy. Batches run in order, one at a time; see "Work Plan and Handoff" for why and for who does what.

**Batch 1 — Xetra consolidation**

- [x] **Step A** — *(implemented and deployed 2026-10-03)* Monthly consolidation: skip-when-current fingerprint, streaming `ParquetWriter`, schema unification, leftover mini-files merged first, unreadable days left out and recorded, never shrink an existing monthly file, dead `time` sort removed. Daily merge: watermark that separates stale mini-files from new ones. Raw-cache cleanup: per-day proof. Tests in `tests/test_xetra_consolidation.py` and `tests/test_xetra_raw_cache.py`. **Must be deployed before 2026-11-01.**
- [x] **Step A2** — *(done 2026-10-03)* One-off repair of the monthly files that are behind their daily data (January, February, May, June 2026). Runbook under "Repairing the monthly files" below. Needs Step A deployed first; run by the owner.

**Batch 2 — Shutdown and logrotate**

- [x] **Step E** — *(implemented and deployed 2026-10-03)* In-loop shutdown checks for both daemons. A `should_stop` callable (`common/shutdown.py`) is passed down: `IntervalScheduler.run` checks it before each ticker, `XetraService.fetch_and_store_missing_trades_incremental` before each date and file, and `_consolidate_to_monthly` between days (an abandoned consolidation deletes its temp file; see Risk Controls for when it is retried). The fetcher's burst cooldown and 429 retry waits sleep in 1-second steps and raise `ShutdownRequested`, which the file loop treats as a stop, not a failed file. A stopped Xetra cycle skips the end-of-cycle consolidation. All three signal handlers now only record the signal; the main loop logs `Received signal N, shutting down gracefully...`. Tests in `tests/test_shutdown_in_loops.py`. Not covered: Yahoo ticker maintenance (`--ticker-maintenance`, weekly by default) and a single running download or day merge.
- [x] **Step F** — *(implemented and deployed 2026-10-03)* Diagnosed: interpreter teardown reading the swapped-out process back from disk; see "What the exit hang turned out to be". Fixed with `exit_daemon_process` (`common/process_exit.py`) at the end of all three daemon loops, and `RuntimeDirectoryPreserve=yes` in the unit templates (`daemon-manage.sh`, `docs/daemon/INSTALLATION.md`). Tests in `tests/test_process_exit.py` and one stop-request test per daemon. After a code review the same day: every idle wait in the three daemon loops now checks for a stop request every 10 seconds (some checked every 60, which is longer than the Xetra units' 30-second stop timeout).
- [x] **Step G** — *(implemented and deployed 2026-10-03)* Logrotate: `copytruncate`, no restart, in `daemon-manage.sh`, `docs/daemon/INSTALLATION.md` and `docs/DAEMON_MODE.md`. `daemon-manage.sh update` did not touch the logrotate config before, so it now replaces an installed config that still contains `postrotate` (function `refresh_stale_logrotate`); no prompt, because the file is generated and carries no local settings.

**Batch 3 — Yahoo disk load and registry** (two deploys: B, C and J together, then D alone)

- [x] **Step B** — *(implemented 2026-10-03, deployed 2026-10-04)* Scoped writes: `PartitionedStorageBackend.merge` rewrites only months that contain a row of the new data and whose merged content differs from the stored file. `save` is unchanged and still used by the legacy migration. Tests in `tests/test_partitioned_storage_backend.py` (untouched months keep modification time and bytes; a touched month keeps its stored rows; duplicates replaced; month boundary touches two; unchanged month not rewritten; rows without a date dropped; damaged file in a touched month fails without overwriting it; an incoming row with fewer values does not replace a stored one; a month that would lose a stored date is not written) and `tests/test_yahoo_scoped_partitions.py` (through `save_single_stock_data`).
- [x] **Step C** — *(implemented 2026-10-03, deployed 2026-10-04)* Scoped reads: `merge` reads one month at a time, by building the file's path (no directory walk). `save_single_stock_data` no longer reads before fetching; it hands the fetched bars to `YFParqed.merge_yf`, which calls `merge` for partitioned storage and read-then-`save` for the legacy layout. `read` and the `StorageInterface` protocol are unchanged.
- [x] **Step J** — *(implemented and deployed 2026-10-04)* Find, record and keep damaged files (Decision 5). Tests in `tests/test_damaged_partitions.py`; the older tests that asserted deletion now assert the rename.
  - [x] **J1** — `safe_read_parquet` (`common/parquet_recovery.py`) reads a second time and then renames an unreadable file to `<name>.damaged-<UTC timestamp>` with `move_aside` instead of deleting it. A missing file is no longer treated as corrupt. If the rename fails, the file stays and is reported as left in place; the legacy backend then raises instead of returning "no data", which would have let its save write over the file. Both backends get this.
  - [x] **J2** — Damage record: `DamageLog` (`common/damage_log.py`) appends to `damaged_partitions.jsonl`. Both backends take an optional `damage_recorder`; `YFParqed` wires it to the working directory, which it looks up at the time of the call because the CLI sets the working directory after building the instance. The migration tool's backends record too.
  - [x] **J3** — `check_partitions` (`common/partition_check.py`) and `YFParqed.check_last_closed_month`, called by the daemon after each cycle that ran to its end; state in `partition_checks.json`.
  - [x] **J4** — `_write_partitions` reads the temp file's footer back before the rename. `GlobalRunLock.cleanup_tmp_files` promotes a leftover temp file only if it reads completely, and otherwise keeps it under a `.damaged-` name.
  - [x] **J5** — `yf-parqed verify-partitions [--month YYYY-MM | --all]` in `yfinance_cli.py`.
- [ ] **Step D** — Registry persistence and the nightly fetch (Decision 6). Not started. Deploy alone; see "Rollout sequence and waits".
  - [ ] **D1** — Registry save: temp file, sync, rename; an empty registry is refused; every 500 tickers and at the end of each cycle in daemon mode; merged onto a fresh read under a lock file; the other writers of `tickers.json` take the lock.
  - [ ] **D2** — Nightly fetch: the time of the last successful fetch per ticker and interval under a new key; fetch when it is older than the most recent 22:00 UTC; always the 7-day fetch; the idle wait ends at 22:00 UTC; `last_data_date` is not written.
  - [ ] **D3** — Failures: `raise_errors=True`; a failed request told from "no data"; per-ticker error handling in `IntervalScheduler.run`; the cycle ends after 20 failures in a row; a summary line names the failed tickers.
  - [ ] **D4** — No automatic pause: a cycle no longer sets the streak, the pause or `permanently_dead`; existing flags are honoured.
  - [ ] **D5** — Tests: a second cycle in the same night fetches nothing; the first cycle after 22:00 UTC fetches every ticker again; a ticker whose request failed is fetched by the next cycle and its saved state is unchanged; a kill in mid-cycle keeps the last periodic save, and the next start fetches only the tickers not yet done; an `add-ticker` during a cycle survives the daemon's next save; an empty registry is not saved; the saved file contains no `last_data_date`.
  - [ ] **D6** — Documentation (Step I), and the exact log lines for the checks under "Checking a deploy".

**Batch 4 — Memory limits**

- [ ] **Step H** — Memory limits in both unit templates (`daemon-manage.sh`, `docs/daemon/INSTALLATION.md`), with values taken from observed peaks. Only after Batches 1 to 3 have been deployed and observed for about a week, and after the November rollover (decided 2026-10-10): the consolidation is the largest memory event and its peak in production has not been measured yet.

**With every batch**

- [ ] **Step I** — Keep `.github/TROUBLESHOOTING.md`, `docs/DAEMON_MODE.md`, `docs/release-notes.md` and this ADR in step with what was changed. When the last batch is done, move this ADR to `implemented/` and update the index.

Run `uv run pytest` after each step; all tests must pass before moving on.

## Work Plan and Handoff

### How the work is split

| Batch | Steps | Session | Model | Why |
|---|---|---|---|---|
| 1 | A | The session that wrote this ADR (2026-10-03) | Opus | Smallest scope, hard deadline, and the context was already loaded. |
| 2 | E, F, G | New session | Opus for F; Sonnet is enough for E and G | F is an open diagnosis. E and G are mechanical and fully specified here. |
| 3 | B, C, J, then D | B and C: session of 2026-10-03. J: session of 2026-10-04. D: another new session, days later | Opus | Storage and fetch logic where a mistake loses data permanently. |
| 4 | H | Any, about a week after Batch 3 | Sonnet | A few lines in two templates; the work is choosing the values. |

**Do not run batches in parallel.** Development happens on the production host (2 cores, 3.7 GiB RAM), so several agents running the test suite compete with the daemons. The shutdown work (E) also touches the same files as Steps A and D. And each batch changes production behaviour that should be watched before the next one lands.

### Starting a session

1. Read this ADR. The Context section holds the evidence; it cannot be regenerated, because `sar` keeps only about a week.
2. Find the lowest batch with an unchecked step. Read the Progress Log below for anything the previous session left open.
3. Read the Risk Controls that mention your steps before writing code.
4. Work the steps in order. Tick each box when its tests pass. Add a dated line to the Progress Log when you stop, including anything unfinished or surprising.

### Next session starts here (written 2026-10-10, after the Step D decisions)

**State of the repository.** Steps B, C and J are deployed and confirmed; production is at `6991084`. No code of Step D is written. Full suite: 656 passed, 1 skipped. This ADR, the new ADR and the index were edited on 2026-10-10 and are for the owner to commit.

**Keep the Yahoo daemon from saving the registry until Step D is deployed.** The weekly ticker maintenance saves the registry from memory when the daemon has been up for 7 days; see Risk Controls. The owner restarted it on 2026-10-10 at 11:06 UTC, which set that clock back: the next maintenance is due on 2026-10-17 at 11:06 UTC. Step D is planned for 2026-10-19, so **one more restart is needed before Saturday 2026-10-17 11:00 UTC** (`sudo systemctl restart yf-parqed`), unless a `daemon-manage.sh update` happens in between, which counts as one. `journalctl -u yf-parqed | grep 'Next run: ~' | tail -1` shows when the next maintenance is due.

**Next is Step D**, in a new session. It is decided: read Decision 6, its sub-steps D1 to D6 under "Sequenced Steps", and the Risk Controls that mention Step D. Nothing is left to decide with the owner before code is written. One thing is to be found out in D3: how cleanly yfinance 0.2.66 with `raise_errors=True` tells a throttled request from a ticker without data.

**After Step D:** [ADR 2026-10-10: Yahoo Ticker Universe from the Exchange Lists](../to-do/2026-10-10-yahoo-ticker-universe-from-exchange-lists.md) (two deploys), and Step H after the November rollover. The order, the dates and the waits are under "Rollout sequence and waits".

**Small and separate, before or after Step D:** the Xetra raw-cache warnings for days without trades (Progress Log, 2026-10-10). Xetra code only; its own commit and deploy.

**Decided by the owner, do not reopen:**

- Quarantine replaces deletion; the automatic check covers the last closed month only; there is no backup of the data, so a damaged closed month can be recorded and kept but not repaired.
- One Yahoo fetch per ticker per night from 22:00 UTC, always for the last 7 days. The Xetra daemon keeps fetching every hour, around the clock; it logs trades outside trading hours.
- No ticker is paused or pruned by a count of empty answers.
- The exchange's own lists replace the datahub files, read daily.
- Renamed instruments are stored under Yahoo's spelling.
- A stable identity across ticker changes (the ISIN mapping work) is for later.
- Step H follows the November rollover.

### Rollout sequence and waits (agreed 2026-10-10)

Dates are the earliest possible; they move when work takes longer. The waits do not shorten. Deploy by day (UTC) and before 22:00, so that an update does not interrupt a nightly cycle.

| # | What | Earliest deploy | Wait before the next deploy | What the wait establishes |
|---|---|---|---|---|
| 0 | Restart `yf-parqed` | done 2026-10-10 11:06 UTC; again before Sat 2026-10-17 11:00 UTC | none | The unreviewed registry save does not happen |
| 1 | Step D | Mon 2026-10-19 | 7 nights, to Mon 10-26 | Every weekday and one weekend captured in full with one fetch a night |
| – | No deploys, no restarts | Fri 10-30 to the rollover check (11-02 to 11-04) | – | Batch 1 at the month change, measured undisturbed |
| 2 | Step H (memory limits) | Thu 2026-11-05 | 4 nights, to Mon 11-09 | Neither daemon touches its limit in normal work |
| 3 | List source and who is asked (new ADR, Step 1) | Mon 2026-11-09 | 7 nights, to Mon 11-16 | Daily list handling works; nobody is dropped by mistake |
| 4 | Yahoo spelling (new ADR, Step 2) | Tue 2026-11-17 | 7 nights, to Tue 11-24 | The renamed instruments deliver bars every night |
| – | No deploys, no restarts | Fri 11-27 to the rollover check (12-01 to 12-03) | – | The first consolidation under memory limits |
| – | First removals by the 30-day rule | about Wed 2026-12-09 | – | The rule removes the silent tickers and none that trade |

**Why 7 nights after Step D.** Yahoo serves 1-minute bars for 7 days. For 7 nights after the deploy, a revert to the previous release refills anything the new code missed, because that release fetches the last 7 days for every ticker in every cycle. After 7 clean nights the first night's bars can no longer be refetched, so the week is both the proof and the last chance to go back without loss. It also covers each weekday once and one weekend.

**Why Step D goes out on a Monday or Tuesday.** A shortfall shows in the next morning's gap check, on a trading day, with most of the week left inside the 7-day window.

**Checks for Step D** (commands under "Checking a deploy"):

- *The deploy day.* No ticker has a saved fetch time yet, so a full cycle starts at once; that is the first test. The night's cycle starts at 22:00 UTC.
- *The next four mornings.* One full cycle began at 22:00 UTC; later cycles asked only for failed tickers; `tickers.json` was saved during the cycle and holds no `last_data_date`; the gap check shows a full session for the day before.
- *The Monday after.* Friday is complete; the weekend cycles ran and wrote close to nothing.
- *The month change* (falls into the no-deploy period). The month-close check of October runs once, after the first cycle that ends in November.
- *The first week of US winter time* (from Mon 2026-11-02). The close moves to 21:00 UTC, one hour before the cycle starts. Check on Tuesday morning that Monday's bars reach 15:59.

**Checks for the others.**

- *Step H.* `systemctl show` reports the limits; `memory.events` of both services shows no `max` and no `oom_kill` after a Yahoo night and a Xetra trading day; the December consolidation completes under the limit.
- *List source.* The number of tickers in the nightly cycle changes only by what is expected (the 14 bogus entries gone, new listings added). The list's creation stamp is the evening before. The log names the unlisted tickers; compare with the 902 and the 76 in the new ADR. Answer **y** to the template question at this deploy.
- *Yahoo spelling.* The dry-run list is read by the owner before the deploy. Afterwards BRK-B and JPM-PL have a directory and a full session each night; the cycle is about 4% longer.

**Going back.**

- *Step D,* within 7 nights: revert on `main` and run the update. The previous release ignores the new keys and fetches everything in every cycle.
- *Step H:* remove the limits from the units.
- *List source:* revert. Tickers that were added stay and are harmless. The rule removes nobody in its first 30 days.
- *Yahoo spelling:* revert. The added instruments are no longer fetched; their files stay.

### Deploying

The agent cannot deploy. Production (`/opt/yf_parqed`) is a checkout of `main`, and `sudo ./daemon-manage.sh update` runs `git pull origin main` before restarting the services. A commit on `develop` therefore reaches production only after it has been pushed **and merged into `main`**. Each batch ends with commits on `develop`; the owner pushes, merges to `main` and runs the update. Record the deploy date in the Progress Log.

**Batch 3 (B, C and J) changes code only.** Answer **N** to the template question. No migration is involved. The update stops both daemons, which now takes seconds.

**Batch 2 changes the unit templates and the logrotate config.** When the update asks "Reinstall systemd service templates ...? [y/N]", answer **y** for this batch. Otherwise the installed units keep deleting the shared runtime directory. The logrotate config is replaced by the update itself; check it afterwards with `cat /etc/logrotate.d/yf_parqed` (it must contain `copytruncate` and no `postrotate`).

Before relying on a deploy, confirm that production has the code. For Step A:

```bash
grep -c '_DAYS_INCLUDED_KEY' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py   # 0 means the old code is still installed
```

### Checking a deploy

- **Batch 1.** The proof arrives at the next month change (2026-11-01 to 11-04). In `/var/log/yf_parqed/xetra-DETR.log*`, `Consolidated to monthly` should appear once for October, and `sar -S` should show swap staying near its baseline. Before that, the dev validation in the Progress Log is the evidence. For the repair (Step A2), compare the row counts with the table under "Repairing the monthly files".
- **Batch 2.** First confirm the code: `grep -c 'should_stop' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py` must print 6 and `grep -c copytruncate /etc/logrotate.d/yf_parqed` must print 1. After the next midnight, `journalctl -u yf-parqed -u 'xetra@DETR' | grep -E 'timed out|SIGKILL'` should show nothing new, and neither service should have restarted at 00:00. A stop during a cycle now logs `Stop requested, ending ...` (Yahoo: `Stop requested during the update cycle`).
- **Step F on its own** (if it is deployed before E and G). First confirm the code and the units:

  ```bash
  grep -c 'exit_daemon_process' /opt/yf_parqed/src/yf_parqed/xetra_cli.py      # must print 3
  systemctl show 'xetra@DETR' yf-parqed -p RuntimeDirectoryPreserve           # must print "yes" twice
  ```

  Then, for every stop of an idle Xetra daemon: `PID file removed` is the last line that process writes to `/var/log/yf_parqed/xetra-DETR.log`, and the journal shows `Deactivated successfully` within about 12 seconds of `Stopping` (up to 10 for the idle loop to notice, 1 to 2 to exit). Until Step E is deployed, a stop that arrives during a cycle is still killed at the timeout; that is expected and is not a Step F failure.
- **Batch 3, after B, C and J.** First confirm the code:

  ```bash
  grep -c 'def merge' /opt/yf_parqed/src/yf_parqed/common/partitioned_storage_backend.py        # must print 1
  grep -c '_verify_temp_file' /opt/yf_parqed/src/yf_parqed/common/partitioned_storage_backend.py # must print 2
  grep -c 'move_aside' /opt/yf_parqed/src/yf_parqed/common/parquet_recovery.py                    # must print 2
  grep -c 'check_last_closed_month' /opt/yf_parqed/src/yf_parqed/yfinance_cli.py                  # must print 1
  ```

  Then, after the first cycle has finished (`All tickers were processed.` in `journalctl -u yf-parqed`):

  ```bash
  journalctl -u yf-parqed --since today | grep -E 'Processing .* tickers|All tickers were processed|Month-close check|Damaged|moved aside'
  cat /var/lib/yf_parqed/partition_checks.json            # one entry, for the last closed month
  ls /var/lib/yf_parqed/damaged_partitions.jsonl          # should not exist
  find /var/lib/yf_parqed/data/us -name '*.damaged-*'     # should print nothing (walks every ticker: a minute or two of disk)
  ```

  To run the check by hand (optional; the daemon does it by itself): `cd /var/lib/yf_parqed && sudo -u yfparqed /opt/yf_parqed/.venv/bin/yf-parqed verify-partitions`. Not with plain `sudo`, see Risk Controls.

  Expected: `Month-close check: reading every stored file of 2026-09` straight after the first cycle that runs to its end, then `Month-close check of 2026-09: ... files, ... rows, 0 damaged, ... seconds` (the numbers measured in dev are in the Progress Log entry of 2026-10-04). The check does not run again in later cycles. Cycle duration (from `Processing ... tickers` to `All tickers were processed.`) should fall below the 4h04m to 4h09m measured on 2026-10-03 and 10-04; what remains is mostly the rate-limited fetching, which Step D addresses. Pick a ticker and list its partition files: only the current month's file may have a fresh modification time after a cycle, and after a cycle that ran while the market was closed, none. If `damaged_partitions.jsonl` exists, follow "Corrupt Parquet Files" in `.github/TROUBLESHOOTING.md` before doing anything to the file it names.
- **Batch 3, after D.** The exact log lines are fixed in D6; until then, what to look for. Each morning for a week:

  ```bash
  journalctl -u yf-parqed --since "yesterday 21:55" | grep -E 'Daemon run #|Processing .* tickers|All tickers were processed'
  ls -l --time-style=long-iso /var/lib/yf_parqed/tickers.json   # written during the night's cycle
  grep -c '"last_data_date"' /var/lib/yf_parqed/tickers.json     # must print 0
  ```

  Expected: one cycle that starts at 22:00 UTC and processes every ticker (about 3.5 hours), then cycles that process only the tickers whose request failed. Then the gap check under "Baseline for the Yahoo gap check": the six liquid tickers have 387 to 390 bars for the day before, and the number of tickers with a bar on that day is close to the same weekday a week earlier. A liquid ticker short of a day, or a clear drop in the number of tickers, means: revert within the 7-day window (see "Rollout sequence and waits").

### Repairing the monthly files (Step A2)

Four monthly files hold fewer trades than their daily files. The repair rebuilds every month from its daily files; months that are already complete come out with the same row count. It rewrites files under `trades_monthly/` and creates two daily files from mini-files, so it needs the owner's go-ahead.

**The repair runs the code installed in `/opt/yf_parqed`, not the code in the dev repo.** Committing the fix on `develop` changes nothing in production. The fix has to travel `develop` → GitHub → `main` → `/opt/yf_parqed` first. With the old code the same command loads a whole month into memory and exhausts RAM and swap; that is what happened on the first attempt on 2026-10-03.

Do the three parts in order. Every command is run on the server as `siggi`.

**Part 1 — get the fix into production (this is "deploying Batch 1")**

```bash
# 1.1 In the dev repo: nothing uncommitted, then publish develop
cd /home/siggi/github/yf_parqed
git status                 # must show a clean working tree on branch develop
git push origin develop

# 1.2 On GitHub: open a pull request from develop into main, wait for CI to pass, merge it.
#     Do NOT create a version tag for this. A tag (v*) is what triggers publishing to PyPI;
#     production only needs the commit on main.

# 1.3 Install main into production. This stops both services, pulls origin/main into
#     /opt/yf_parqed, syncs dependencies, clears the bytecache and starts both services again.
cd /home/siggi/github/yf_parqed
sudo ./daemon-manage.sh update
#     It asks "Reinstall systemd service templates ...? [y/N]"  -> answer N.
#     It may ask about a pending migration or registry pruning -> answer as you normally do;
#     neither is related to this repair.
```

**Part 2 — confirm production has the fix**

```bash
grep -c '_DAYS_INCLUDED_KEY' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py
```

This must print `3`. If it prints `0`, production still has the old code: stop here and go back to Part 1.

For the second run on 2026-10-03 (the June fix), also check that the merge fix and the integer contract are installed; this must print `2` and then `2`:

```bash
grep -c '_usual_schema' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py
grep -c '_enforce_integer_contract' /opt/yf_parqed/src/yf_parqed/xetra/xetra_service.py
```

**Part 3 — run the repair**

Run it outside trading hours. The update in Part 1 started both services again, so stop the Xetra collector first; only one process should merge mini-files.

```bash
# 3.1 Stop the Xetra collector
sudo systemctl stop 'xetra@DETR'

# 3.2 Backup: already done. trades_monthly.bak-2026-10 was made on 2026-10-03 before the
#     first attempt and is complete. Do not copy again; that would overwrite it.
ls /var/lib/yf_parqed/data/de/xetra/trades_monthly.bak-2026-10/venue=DETR    # must list year=2025 and year=2026

# 3.3 Rebuild every month.
#     The cd is required: consolidate-month ignores --wrk-dir and looks for ./data.
#     From any other directory it reports "No months found" and does nothing.
cd /var/lib/yf_parqed
sudo -u yfparqed /opt/yf_parqed/.venv/bin/xetra-parqed consolidate-month DETR --all

# 3.4 Check the row counts against the table below
sudo -u yfparqed /opt/yf_parqed/.venv/bin/python3 -c "
import pyarrow.parquet as pq, pathlib
for f in sorted(pathlib.Path('/var/lib/yf_parqed/data/de/xetra/trades_monthly').rglob('trades.parquet')):
    print(f.parent.parent.name, f.parent.name, format(pq.read_metadata(f).num_rows, ','))"

# 3.5 Start the Xetra collector again
sudo systemctl start 'xetra@DETR'
```

What to expect in step 3.3: one to one and a half minutes per month, about fifteen minutes for all ten, with memory use staying under 1 GB. If a single month runs for more than five minutes or `free -m` shows swap climbing, press Ctrl+C: the old code is running, and Part 2 was skipped or failed.

Expected row counts, taken from the daily files on 2026-10-03:

| Month | Before | After | Change |
|---|---|---|---|
| 2025-12 | 5,224,813 | 5,224,813 | none |
| 2026-01 | 8,766,890 | 9,285,530 | +518,640 |
| 2026-02 | 6,507,722 | 9,495,685 | +2,987,963 |
| 2026-04 | 503,888 | 503,888 | none |
| 2026-05 | 9,766,173 | 10,267,787 | +501,614 (day 8 merged from 830 mini-files) |
| 2026-06 | 10,628,160 | 11,190,760 | +562,600 (day 10 merged from 831 mini-files) |
| 2026-07 | 10,038,312 | 10,038,312 | none |
| 2026-08 | 7,978,227 | 7,978,227 | none |
| 2026-09 | 8,140,348 | 8,140,348 | none |
| 2026-10 | no file | 917,190 or more | new; the current month is included by `--all` and is rebuilt at the November rollover |

Two things change besides row counts. Every rebuilt file gains the fingerprint and day list in its metadata. And the four oldest files (2025-12 to 2026-04), which were written by the May 2026 migration without the `venue`, `year`, `month` and `day` columns, come out with those columns, like the files from May onward.

If a count is lower than expected, nothing was lost: the never-shrink guard refuses to replace a file with a smaller one, and the backup is still there. Delete the backup (`sudo rm -r /var/lib/yf_parqed/data/de/xetra/trades_monthly.bak-2026-10`) once the counts are confirmed.

### Baseline for the Yahoo gap check (measured 2026-10-03)

Count 1-minute bars per trading day for a few liquid tickers by reading the `date` column of `/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=<T>/`.

- AAPL, MSFT, NVDA, JPM, XOM and ZTS each had 214 trading days from 2025-11-25 to 2026-10-02 with a median of 390 bars per day (a full session).
- No business day was missing except market holidays. Half-day sessions (2025-11-28, 2025-12-24) have 196 to 210 bars.
- One known anomaly: 2026-01-06 has only 293 to 323 bars for all six. Cause unknown; it predates this work.
- Thinly traded instruments (SPAC units and the like) legitimately have gaps of weeks. Use liquid tickers for the check.
- For the whole registry: about 7,335 tickers had a bar on Friday 2026-10-09 (their October file was written during or after that session; 409 others have an October file but did not trade that day, and 1,533 have no October file).

After Step D, any liquid ticker with a missing business day or a day well short of 390 bars is a regression. Yahoo serves only the last 7 days of 1-minute data, so act within that window: revert Step D and let the old fetch-everything behaviour refill the gap.

### Progress Log

- **2026-10-03** — Investigation done, ADR written, steps ordered into batches.
- **2026-10-03** — Step A implemented in `XetraService._consolidate_to_monthly` with tests in `tests/test_xetra_consolidation.py`; full suite 541 passed, 1 skipped. Validated in dev against the real September 2026 daily files (read-only, output to a scratch directory): 59 seconds and 326 MB peak memory, against 6 to 70 minutes and the whole machine before. The result has the same 8,140,348 rows as the production monthly file, an equal schema, and all 21 columns identical value for value and in the same order. A second call returned in 9 ms. **Left open:** the changes are in the working tree on `develop`, not committed and not deployed. Next: commit, deploy before 2026-11-01, then start Batch 2 in a new session.
- **2026-10-03** — Finding while reviewing how an unreadable daily file is handled. No Parquet file has ever been unreadable (167 daily and 9 monthly footers all read; no read errors in the retained logs). A different gap was real: 2026-05-08 and 2026-06-10 never got a daily `trades.parquet`. Their trades were intact in mini-files but missing from the May and June monthly files. Why those days were never merged is unknown; the logs from then are gone. Their raw cache had been deleted because cleanup accepted any readable monthly file as proof for the whole month. The January and February 2026 monthly files are also behind their daily files (by 518,640 and 2,987,963 trades); they were written on 2026-05-01 and the daily files changed afterwards.
- **2026-10-03** — Step A extended to cover this (see Decision 1): leftover mini-files merged before consolidating, stale-versus-new watermark in the daily merge, unreadable days left out on the record, never-shrink guard, per-day raw-cache proof. Full suite 549 passed, 1 skipped. Rehearsed on scratch copies of real data: May 2026 went from 9,766,173 to 10,267,787 trades with day 8 included (85 seconds), February 2026 from 6,507,722 to 9,495,685 (75 seconds), peak memory 542 MB, repeat calls skipped in 11 ms. **Left open:** the extension is in the working tree, not committed; nothing is deployed; Step A2 (the repair) has not been run.
- **2026-10-03** — The repair (Step A2) was attempted before Step A had reached production and had to be abandoned. Production was still on `main` at commit `6928491` (2026-05-02); the Step A commits were on local `develop`, unpushed. So `consolidate-month --all` ran the old pandas consolidation: December 2025 completed, January 2026 exhausted memory, and the kernel OOM-killed the process at 08:52 UTC (3.2 GB resident). **No data was damaged:** December was rewritten with the same 5,224,813 rows and identical content in every column (checked against the backup); January and all later monthly files are untouched; no temp file was left behind; the mini-files of 2026-05-08 and 2026-06-10 are untouched; the backup `trades_monthly.bak-2026-10` is complete. Both services were stopped by the owner beforehand and are still stopped. The runbook now starts with a check that production has the new code, and the Deploying section now states that production follows `main`. **Left open:** Step A2 from Part 1 of its runbook (push `develop`, merge to `main`, run the update, confirm, repair).
- **2026-10-03** — Batch 1 deployed (develop merged to `main`, update run) and Step A2 run by the owner at about 09:15 UTC, with no memory pressure. Nine of ten monthly files now match the expected row counts and carry the fingerprint and day list; 2026-05-08 was merged from its mini-files (501,614 rows). **June is not repaired yet:** merging the mini-files of 2026-06-10 failed with `Schema at index 796 was different`. Two of its 831 mini-files have `price_notation` as `double` instead of `int64`, because those files contain trades with no price notation (7 trades in total) and pandas stores an integer column with missing values as float. The daily merge required identical schemas, so this day could never be merged; that is why it was stuck since June. The mini-files are untouched and June's monthly file was rebuilt without day 10 (10,628,160 rows, as before).
- **2026-10-03** — Fix for the above, in `_consolidate_daily_files`: tables are brought to the schema most mini-files have when the cast loses nothing (here `double` back to `int64`, missing values kept as nulls); if a value does not fit, the column is widened instead. Two tests added; full suite 551 passed, 1 skipped. Rehearsed on a scratch copy of all of June: 10,628,160 to 11,190,760 trades, day 10 included with 562,600 rows, `price_notation` still `int64` with 7 nulls, schema equal to the neighbouring days, 78 seconds, 472 MB peak. **Left open:** this fix is in the working tree, not committed and not deployed. To finish Step A2, repeat Parts 1 to 3 of the runbook; in Part 2 the check below must print a number above 0, and in Part 3 only June is rebuilt (the other months report as current and are skipped in milliseconds). Not addressed: the parser still produces the widened type for such files; the merge now absorbs it.
- **2026-10-03** — The owner asked for the data contract to be enforced on that column instead of absorbed, so the point above is now addressed. `price_notation` is under an integer contract (`XetraParser.INTEGER_FIELDS`): the parser always produces a nullable integer; a missing value is a null; a value that is not a whole number is rounded (not a number at all: null), logged as an error, and the original file is copied to `contract_violations/`, which no cleanup touches. The daily merge applies the same rule to mini-files staged before the contract existed. Checked against stored data first: all 47.6 million stored values are the integer `1` (April to October 2026); the text value in the old parser test fixture was invented and is now `1`. 40 real raw files (86,243 trades) parse with no violation; the June 10 rehearsal still gives 562,600 rows, `int64`, 7 nulls, nothing copied to `contract_violations/`. Full suite 561 passed, 1 skipped. Documented in `.github/STORAGE_STRUCTURE.md` and `docs/DATA_MODEL.md`. **Left open:** in the working tree, not committed, not deployed; Step A2 for June still to be run after the deploy.
- **2026-10-03** — Contract checked against the regulation before committing. MiFIR RTS 1, Annex I, Table 3 defines "Price notation" as one of four codes (`MONE` monetary value, `PERC` percentage, `YIEL` yield, `BAPO` basis points; `MONE` for shares and ETFs). Deutsche Börse's delayed JSON sends a number instead; that `1` means `MONE` is inferred, because their public pages carry no field mapping (data.services@deutsche-boerse.com is the contact). Since the field is a category, the rule for values that are not whole numbers was changed from rounding to storing null; logging and keeping the original are unchanged. Accepted risk, agreed with the owner: if Deutsche Börse switches to the text codes, every value becomes null and every file is kept under `contract_violations/` until the parser is updated and the files are re-parsed. Full suite 562 passed, 1 skipped.
- **2026-10-03** — Batch 1 complete. All of the above is committed on `develop`, merged to `main` and installed in `/opt/yf_parqed` (both runbook checks print `2`). Step A2 was run again and finished without errors. Verified afterwards: all ten monthly files match the expected row counts and carry the fingerprint and day list; June is at 11,190,760 trades with 2026-06-10 included (562,600 rows, `price_notation` as `int64` with 7 nulls); no mini-files or temp files are left anywhere; `contract_violations/` does not exist, so nothing broke the contract. **Left open:** (1) both services were still stopped when this was checked (`yf-parqed`, `xetra@DETR`); they must be started before Monday's trading. (2) The backup `trades_monthly.bak-2026-10` (2.0 GB) is still on disk; the owner deletes it when satisfied. (3) The proof under real conditions is the November rollover. (4) Next session: Batch 2 (Steps E, F, G).
- **2026-10-03** — Batch 2, Step F done (Steps E and G not started). Cause: the idle daemon's memory is in swap when the host is short of RAM, and Python's exit reads it all back from the hard disk in order to free it; see "What the exit hang turned out to be" in the Context. Reproduced in dev (15 to 16 seconds from the end of the loop to the end of the process, about 11,000 major page faults) and fixed (about 1 second, about 900 faults) by ending the process with `os._exit` after cleanup, in all three daemon loops. The missing `PID file removed` line was a second defect: the shared `RuntimeDirectory` is deleted when either service stops; both templates now set `RuntimeDirectoryPreserve=yes`. Full suite 569 passed, 1 skipped. **Left open:** (1) in the working tree on `develop`, not committed, not deployed; when deploying, answer **y** to the template question. (2) Steps E and G. Step E has two additions from this diagnosis, noted in its step text. (3) Both services were stopped when this session ended (`yf-parqed` shows `failed` from the last manual stop, `xetra@DETR` is inactive); they must be started before Monday's trading.
- **2026-10-03** — Code review of Step F and follow-up. Fixed: idle waits that checked for a stop only every 60 seconds (now 10 everywhere); the test fixtures (later tests keep a log sink; the stop fixture no longer fails when a sleep comes before the handler is registered). Not changed, on purpose: the template prompt in `daemon-manage.sh update` stays manual; the `atexit` PID cleanup stays, because it is what removes the PID file when the Xetra preflight check refuses to start; the wrapper alternative is recorded under Alternatives Considered. Full suite 569 passed, 1 skipped. **Found on the side:** the ISIN mapping daemon is not installed in production and cannot be installed as it is. The Deutsche Börse page it reads the CSV link from now redirects to an address that answers 404, so `update-isin-mapping` fails on its first request. `docs/daemon/INSTALLATION.md` now says so. It is outside this ADR.
- **2026-10-03** — Batch 2, Steps E and G done; Batch 2 is complete. Step E: see its step text for the design. Step F's finding that midnight stops land in the burst cooldown matches the fetcher: `enforce_limits` did a single `time.sleep(35)` every 30 requests; it now sleeps in 1-second steps. A `ShutdownRequested` exception is used for the fetcher (rather than letting the download proceed after a cut-short cooldown) because a request sent without its cooldown risks a 429 and its uninterruptible backoff. Step G: the Xetra daemon already rotates its own log at 10 MB (loguru `rotation`/`retention`/`compression` in `xetra_cli.py`); logrotate's daily `copytruncate` runs alongside it. Verified in dev with a real loguru file sink and `logrotate -f`: the sink keeps writing after the truncate, the new file starts clean (no NUL padding), the copy is `a.log.1`. `logrotate -d` accepts the config. Full suite 586 passed, 1 skipped. **Left open:** (1) in the working tree on `develop`, not committed, not deployed; answer **y** to the template question. (2) Ticker maintenance and a single in-flight download or day merge still run to completion on a stop; accepted, see Risk Controls. (3) The Step F left-open note about both services being stopped still applies if they have not been started since. A code review the same day led to: `ShutdownRequested` re-raised in `fetch_all_trades_for_date`, an accurate initial-fetch log line on a stop, the Yahoo stop message reworded to `Stop requested during the update cycle` (it cannot tell a cut-short cycle from one that finished just before the signal), and the two Risk Controls above. Not acted on: passing `should_stop` to the fetcher constructor (the current form also covers an injected fetcher). Cleaned up afterwards at the owner's request: the three copies of the signal handler and the four hand-written chunked sleeps (one of which had drifted to 60-second steps in Step F) are now one `StopFlag` in `common/shutdown.py` (`install`, `log_request`, `sleep`, and calling it answers "stop requested?"), and the scheduler's two stop guards share one local `stopping()` so both exits log the same line. The idle check interval (10 s) is `IDLE_CHECK_SECONDS` there. Behaviour is unchanged; tests that patched `yf_parqed.xetra_cli.time.sleep` now patch `time.sleep`.
- **2026-10-03** — Batch 2 deployed; verified before starting Batch 3. Production HEAD is `166983f` (merge of PR #5, containing `2363da0`). `should_stop` count 6, `exit_daemon_process` count 3, `copytruncate` 1, `postrotate` 0; both installed units have `RuntimeDirectoryPreserve=yes`; `yf-parqed` and `xetra@DETR` active since 12:32 UTC (so the services are running again, which closes the "stopped services" notes above). **Not yet observed:** the midnight stop; check it on 2026-10-04 with the Batch 2 recipe under "Checking a deploy". Batch 3 was started the same day, before that night; Steps B and C touch Yahoo storage code, which Batch 2 did not.
- **2026-10-03** — Batch 3, Steps B and C done; Step D not started. First version: `save` wrote only the months of `new_data` and `read` took a `months` argument, with the caller reading and passing the touched months. A code review the same day found that this let a caller shrink a month by passing too little, that months were rewritten even when nothing changed, and that a fetched row without a date would crash the scoped read. Reworked into one method, `PartitionedStorageBackend.merge`, which reads the touched months itself, drops rows without a date and skips a month whose merged rows equal the stored ones; `save`, `read` and the storage protocol are back to what they were, and `YFParqed.merge_yf` chooses between `merge` and the legacy read-then-save. **Checked on real data** (scratch copies, production untouched): a live 7-day 1-minute fetch for AAPL and ZTS on Saturday 2026-10-03 (2,730 and 2,721 bars, no row without a date) matched the stored bars exactly, so nothing was written and no file changed. For one AAPL cycle the storage work went from 1,182 ms CPU and 37 MB of allocations (read 12 files, rewrite 12) to 79 ms and 3.5 MB with nothing new, or 101 ms and 6.4 MB with one new bar (one file written). If every one of the 9,276 tickers were like AAPL (12 monthly files, 83,003 rows), that would be about 3 CPU-hours of storage work per cycle before and about 15 minutes after; many tickers are smaller, and the fetch itself is unchanged until Step D. Existing tests that used `save_yf` as the place to intercept a cycle's write now use `merge_yf`. At the owner's request `merge` also got two guards against replacing captured data with less (see Decision 2). The first one answers something real: in a sample of 400 September files (1,180,641 bars), 21 stored bars have no prices at all, so Yahoo does send such bars. A live refetch with the guards in place still wrote nothing and logged no ignored rows. Full suite 611 passed, 1 skipped. **Left open:** (1) committed on `develop` as `51b8654`, not deployed; B and C deploy together with Step J, Step D waits until they have run cleanly for a few days. (2) The cycle duration gain is unmeasured until deployed. (3) Recorded under Risk Controls and not acted on: nothing detects a damaged file in a closed month any more (a strategy is being agreed with the owner), and one failing ticker ends the cycle. (4) Found while looking at this: `safe_read_parquet` deletes a file it cannot read, and it treats any `OSError` as "corrupt". For a touched month that is a whole month of a ticker's 1-minute bars, of which at most 7 days can be fetched again. Not changed yet; to be decided with the damaged-file strategy.
- **2026-10-03** — Agreed with the owner at the end of the session: build Step J (Decision 5) before deploying B and C, in a fresh session. There is no regular backup of `/var/lib/yf_parqed/data`. Nothing of Step J is started. The handoff is under "Next session starts here". **Left open:** (1) B and C are committed on `develop` (`51b8654`) and must not reach `main` before Step J is done. (2) The first midnight with Batch 2 (2026-10-04 00:00) has not been checked. (3) Scratch copies and probe scripts from this session were in the session's scratch directory and are not needed again; the measurements are in this log and under Risk Controls.
- **2026-10-04** — First midnight with Batch 2 checked at 08:20 UTC. **Step G confirmed:** logrotate ran at 00:00:02 and neither service was stopped. Both still run with the PIDs from the deploy (`yf-parqed` 1849884, `xetra@DETR` 1849896, started 2026-10-03 12:32 UTC, `NRestarts=0`), and the journal has no systemd line for either unit since then, so no `timed out` and no `SIGKILL` (every night from 2026-09-24 to 2026-10-03 had both). The Xetra log was rotated to `xetra-DETR.log.1` by `copytruncate`; the daemon kept writing to the truncated file, which starts with a whole line and contains no NUL bytes. Production is still at `166983f`. **Not proven yet: Steps E and F.** No stop has been sent to the new code, so the in-loop checks and the fast exit have not run in production. Check them at the first stop (the next `daemon-manage.sh update`, or a manual restart) with the Batch 2 and Step F recipes under "Checking a deploy". Check there too that the Yahoo daemon starts its cycle straight away: at the 12:32 start on 2026-10-03 it found the lock of a process that had been SIGKILLed at 08:29, and the automatic recovery (`GlobalRunLock.cleanup_tmp_files`, a walk over every ticker directory) took 16 minutes (12:33 to 12:48) before the cycle began. A clean exit should leave no lock behind. **Other observations.** Xetra: 21 hourly runs, INFO lines only, nothing to fetch (weekend), no consolidation, monthly files unchanged since the repair; this says nothing new about Batch 1, whose proof is the November rollover. Yahoo: three complete cycles of 4h09m, 4h06m and 4h04m, the same as the 4h08m baseline, as expected with Batch 3 not deployed (AAPL's August file is still rewritten every cycle). Memory since the deploy: process peak RSS 287 MB (Yahoo) and 144 MB (Xetra, weekend only); cgroup swap peaks 4 MB and 32 MB; cgroup memory peak of `yf-parqed` 2.36 GB, mostly page cache. Host swap was flat at 77 to 88 MB from 14:30 on; iowait is about 14% during a Yahoo cycle and under 1% between cycles. Host swap rose to 627 MB between 13:30 and 14:10 on 2026-10-03; the cgroup peaks rule out the daemons, and the Batch 3 dev session was running then. **Found on the side, not changed:** `yfinance_cli.py` logs the literal text `Recovered %d tmp files` (lines 401 and 415; loguru does not fill in `%d`); the backup `trades_monthly.bak-2026-10` is still on disk; a unit `xetra-detr.service` from December 2025 still shows as failed (`sudo systemctl reset-failed xetra-detr.service` clears it).
- **2026-10-04** — **Steps E and F confirmed in production.** The owner ran `sudo systemctl restart 'xetra@DETR' yf-parqed` at 08:24:43 UTC, with the Xetra daemon idle between runs and the Yahoo daemon in the middle of a cycle (started 07:08). Both units reached `Deactivated successfully` at 08:24:45, two seconds after `Stopping`, with `Result=success`; no `timed out`, no `SIGKILL`. Xetra wrote `Received signal 15`, `Daemon shutting down gracefully` and, as its last line, `PID file removed`. Yahoo wrote `Stop requested, ending the update cycle`, `Stop requested during the update cycle.`, `Daemon shutdown complete.` and `PID file removed`. Both services stopped at the same moment and `/run/yf_parqed` survived, so `RuntimeDirectoryPreserve=yes` works; both PID files were written again by the new processes (Xetra 2224524, Yahoo 2224531). The run lock was released: the new Yahoo daemon logged no `Another update or migration run appears to be in progress` and began its cycle 8 seconds after the start (08:24:53), against 16 minutes after the killed process the day before. Batch 2 needs no further checks. Seen in the same output and expected until Step D: the stopped cycle logged `Tickers file was not updated.`, so what that cycle learned about the tickers it had processed was not saved.
- **2026-10-04** — Batch 3, Step J done (J1 to J5); Step D not started. What was built is in the step text and Decision 5. Full suite 656 passed, 1 skipped (45 new tests in `tests/test_damaged_partitions.py` and `tests/test_cleanup_expanded.py`; five older tests that asserted deletion now assert the rename and the kept bytes). **Checked on real data** (production untouched): the new check read all of September 2026 straight from `/var/lib/yf_parqed/data`, read-only: 7,889 files, 25,317,591 rows, none damaged, 156 MB peak memory, 28 seconds of CPU, 434 seconds of wall time while a daemon cycle and the test suite were using the same disk (the daemon runs it between cycles, so expect less). On scratch copies of AAPL and ZTS: `verify-partitions` for one month, the default month and `--all`; a 4 KiB hole in a September file is found, recorded once across repeated runs and left untouched; with the October file damaged, the first merge fails and renames it, the second writes a new file equal to production's, and a full read of the ticker skips the renamed file; a refetch of stored bars still writes nothing (37 ms) and one new bar costs one verified write (121 ms). The three recipes in `.github/TROUBLESHOOTING.md` (inspect, merge back, probe columns) were run on those copies. Corruption probe on a copy of a real file: see Risk Controls. **Code review the same day, ten findings.** Fixed: the legacy backend returned "no data" for an unreadable file that could not be renamed, so its save would have written over it (it raises now); an incomplete leftover temp file was deleted (it is kept under a `.damaged-` name now, and a `MemoryError` while reading it no longer counts as "incomplete"); one failed write to the damage record stopped the month from being noted, which would have repeated the check in every cycle (the two files are written independently now); a damaged file was recorded once per path for ever (now once per path, error, size and modification time, and paths are stored absolute); a byte that is not text in either file raised (both are read tolerantly now); the migration tool's backends had no recorder; the check ran after a failed cycle too (now only after one that ran to its end); the full-file read existed twice. Taken up in a smaller form: a read error is retried once before the file is moved. Not changed: the check still counts any read failure as damage, including a permission or I/O error, because for a file the daemon cannot read that is the record the owner needs. **Left open:** (1) Step J and its documentation are in the working tree on `develop`, not committed, not deployed. (2) B, C and J deploy together; answer **N** to the template question. (3) After the deploy: the checks under "Checking a deploy", Batch 3. (4) Step D after a few clean days.
- **2026-10-04** — B, C and J deployed by the owner at 11:13 UTC (commit `a9d1d8f` on `develop`, merged as PR #6; production HEAD `6991084`). Code markers as expected (`def merge` 1, `_verify_temp_file` 2, `move_aside` 2, `check_last_closed_month` 1). The update stopped both daemons cleanly: Yahoo in 2 seconds in the middle of a cycle (`Stop requested, ending the update cycle`), Xetra in 4, no `timed out`, no `SIGKILL`; this is the second clean stop of the Batch 2 code. Both started at 11:13:38 with no stale lock, and the first cycle on the new code began at 11:13:42 with 9,277 tickers. First sign of Steps B and C, 90 seconds in: the first 40 tickers of the cycle have 450 partition files and none was written by the new code (markets are closed, so every fetched bar is already stored), where the old code's cycle at 08:24 had rewritten all 12 files of each ticker; no temp or `.damaged-` files. **Not yet observed:** the end of that cycle, its duration, and the month-close check of September 2026 that follows it.
- **2026-10-10** — **Steps B, C and J confirmed after six days in production** (checked at 05:45 UTC, read-only). Production is still at `6991084`; both services run since the deploy with `NRestarts=0`, and the journal has no systemd line for either unit since then. Yahoo: 26 complete cycles, no warning and no error line. Cycle duration 3h26m to 3h28m, against 4h04m to 4h09m before; weekday and weekend cycles take the same time, so what remains is the rate-limited fetching. The month-close check ran once, straight after the first cycle (2026-10-04 14:40:16 to 14:43:06): 7,889 files, 25,317,591 rows, 0 damaged, 170 seconds, the same counts as in dev; `partition_checks.json` has that one entry and `damaged_partitions.jsonl` does not exist. AAPL's files for May to September were last written by the old code on 2026-10-04 08:32; only October changes. Gap check: AAPL, MSFT, NVDA, JPM, XOM and ZTS have 387 to 390 bars on every business day from 2026-09-24 to 2026-10-09, none missing. Hourly iowait on 8 and 9 October is 0.4% to 2.9% (about 14% during a cycle before); host swap has stayed under 130 MB since 6 October. Peaks since the deploy, over a full trading week: Yahoo process 200 MB resident, cgroup 1.04 GB (2.36 GB before, mostly page cache), cgroup swap 15 MB; Xetra process 300 MB resident, cgroup 415 MB, cgroup swap 69 MB. **Not run:** the `find -name '*.damaged-*'` walk over every ticker, to leave the disk to the daemon. **Looked at for Step D, nothing changed:** `tickers.json` was last written at the start on 2026-10-04 (9,277 tickers, all active, none with a `last_data_date`); every cycle ends with `Tickers file was not updated.`; about 1,130 tickers returned no data in the session cycle of 8 October and 1,543 in the cycle that began on 9 October at 21:59. What the code does with a saved registry is in the two Step D Risk Controls and the new one on outages; the three decisions that follow are under "Next session starts here". **Found on the side, Xetra.** (1) Raw-cache warnings for days without trades. The 60 files of Sunday 2026-09-27 (`T23_00` to `T23_59`, 20 bytes each) are older than 7 days, and no Parquet file will ever hold that day, so the per-day proof of Step A keeps them and logs 60 warnings in every hourly run, 1,440 a day. Sunday 2026-10-04 has 60 such files as well and joins when they are 7 days old; one more Sunday every week. No data is at risk, but the warnings bury real ones. Not changed. (2) On Friday 2026-10-09 at 22:59 UTC, 49 files of the hour `T23_xx` answered 404 and the day was logged as partial; its daily file was written at 21:43 and has the usual size, and its raw directory has 1,261 files against 1,321 on the other weekdays. The same on the Fridays 2026-09-25 (40 files) and 2026-10-02 (32), so it is older than this work. Not looked into further. **Housekeeping.** The `Recovered %d tmp files` message in `yfinance_cli.py` (two places) now prints the number; full suite 656 passed, 1 skipped. The owner decided to delete the backup `trades_monthly.bak-2026-10`; compared first, footers only: all nine files have a live monthly file with at least as many rows. The unit `xetra-detr.service` is what is left of the first installation on 2025-12-01, before the template unit `xetra@.service`: its unit file is gone and only the failed state of its last stop on 2025-12-04 remains (`sudo systemctl reset-failed xetra-detr.service`). **Left open:** (1) this entry, the handoff above and the log message are in the working tree on `develop`, not committed; the log message needs no deploy of its own. (2) Whether the backup and the failed unit are gone is not checked; both commands need `sudo`. (3) Step D, after the three decisions. (4) The raw-cache warnings. (5) The November rollover.
- **2026-10-10** — **Step D decided with the owner; no code written.** The decisions are Decision 6; what follows Step D is in [ADR 2026-10-10: Yahoo Ticker Universe from the Exchange Lists](../to-do/2026-10-10-yahoo-ticker-universe-from-exchange-lists.md); the proposed order of deploys is under "Rollout sequence and waits". Measurements behind the decisions, all read-only. **(1) The weekly maintenance will save the registry.** `run_ticker_maintenance` calls `update_current_list_of_stocks`, which saves the registry from memory, and after a cycle that memory holds what the cycle learned. Maintenance runs at every start and then when 7 days have passed; with the nightly restart it only ever ran at a start, on a registry just loaded from disk. The daemon has been up since 2026-10-04 11:13 UTC and logged `Next run: ~2026-10-11 11:13`. Read from the code, not run. **(2) A second fetch after the close adds almost nothing.** Last write of each ticker's October file, taken on Saturday 2026-10-10 at 07:05 UTC: 409 before Friday's session, 2,007 during Friday's in-session cycles, 5,314 in the first cycle after the close (run #25, 21:59 to 01:26 UTC), 14 in the second (run #26, 03:26 to 06:52 UTC), and 1,533 tickers have no October file. So the second pass changed 14 of 7,744 files, all thin tickers; what they gained is not known. One night only. **(3) Stored bars are regular-session bars.** AAPL, MSFT, NVDA, JPM, XOM, ZTS, SPY and TSLA, September and October 2026, 76,396 bars: all between 09:30 and 15:59 exchange time, none on a weekend. The fetcher does not ask for pre-market or after-hours bars. **(4) yfinance** reads a date without a time zone as exchange time (`_parse_user_dt`, 0.2.66), and has `raise_errors` with separate `YFRateLimitError` and `YFPricesMissingError`; how they behave under throttling is not tested. **(5) The exchange lists and the symbol spellings:** see the Context of the new ADR. **Left open:** (1) the restart before 2026-10-11 11:00 UTC, by the owner. (2) The rollout sequence is proposed, not agreed. (3) Step D, in a new session.
- **2026-10-10** — **Rollout sequence agreed by the owner; first restart done.** The sequence under "Rollout sequence and waits" stands as written. The owner restarted `yf-parqed` at 11:06:22 UTC, in the middle of a cycle: stopped and started within the same second, `Result=success`, no `timed out`, no `SIGKILL`, PID file removed; the third clean stop of the Batch 2 code. The maintenance at the start saved a registry just loaded from disk (9,278 tickers, no `last_data_date`), logged `Next run: ~2026-10-17 11:06:32`, and the cycle began 10 seconds after the start. **Left open:** (1) a second restart before 2026-10-17 11:00 UTC, by the owner. (2) Step D, in a new session, for a deploy on Monday 2026-10-19. (3) This ADR, the new ADR and the index are in the working tree on `develop`, not committed.

## Risk Controls

- **Daily files stay the record.** Consolidation never deletes or modifies daily files. Before deploying Step A, run the new consolidation in dev against a copy of a real month and compare row count and per-ISIN counts with the existing monthly file.
- **Raw-cache cleanup became stricter, not looser.** It deletes a raw file only when that specific day is proven to be in a readable daily file or listed in a monthly file. Until Step A2 has rebuilt the older monthly files, they carry no day list and prove nothing; this only matters for days without a daily file, and currently every day has one except the two being repaired.
- **The daily merge now keeps data it used to delete.** Mini-files found next to an existing daily file are merged when they were staged after it. If the system clock is set backwards between a crash and the next run, an already-merged mini-file could be merged twice. That needs a crash and a clock step together; it is accepted.
- **Scoped reads change what a damaged partition does.** Before, one unreadable partition anywhere in a ticker failed the whole read, and a corrupt file was deleted on that read. Now a damaged file in a month the cycle does not touch is not opened: it neither blocks the update nor is deleted. **The daemon will not find it afterwards either**, because it only ever opens the current month (and the previous one in the first week of a month). The full-ticker read was the only thing that noticed such a file, as a side effect. If detection is wanted, it needs a check of its own; none exists today. Measured on 2026-10-03 for the last closed month (September: 7,889 files, 485 MB): reading a file completely with PyArrow takes about 16 ms of disk time and 7 ms of CPU once its footer is cached, and a footer alone about 16 ms of disk time, so a full check of one closed month is a few minutes of disk and under a minute of CPU, one file in memory at a time. A check of all months (93,472 files, 4.8 GiB) is in the order of an hour of disk. In the journal from 2026-09-04 to 2026-10-03 the full read never reported a damaged partition. A damaged file in a touched month behaves as before: the ticker's update fails, and a schema problem leaves the file in place.
- **Detection now has a check of its own (Step J), with limits.** The month-close check reads the last closed month once; older months only when `verify-partitions` is run. It finds files that cannot be decoded. Yahoo partition files are gzip-compressed, and gzip carries a checksum: of 300 single-bit flips applied to a copy of a real file, 294 made the read fail, 5 left the content identical (they hit bytes the reader does not use), and none produced different data silently; zeroed 4 KiB blocks and truncations failed every time. The check cannot judge values that decode but are wrong, and it says nothing about a file that is rewritten after it was checked: the previous month's files can still be rewritten during the first week of a month, when a refetched bar differs from the stored one. Those writes are covered by the read-back of the temp file (footer and row count), not by a full decode.
- **A file is moved aside on any read error that happens twice in a row.** `safe_read_parquet` cannot tell a damaged file from a disk or permission error that lasts; the second attempt only filters out errors that pass at once. Leaving such a file in place instead was considered and rejected: the ticker would fail in every cycle and, as things stand, end the cycle for every ticker after it. After a false alarm the month has two files: the old one under its `.damaged-` name and a new `data.parquet` holding what Yahoo still served. Nothing is lost, but they have to be merged by hand (recipe in `.github/TROUBLESHOOTING.md`); the line in `damaged_partitions.jsonl` is what tells the owner. If the rename itself fails, the file stays, the ticker's update fails, and nothing is written over it.
- **A leftover temp file is judged by reading it.** `cleanup_tmp_files` runs when a stale run lock is found, which after Batch 2 means after a kill. With a `data.parquet` next to it the temp file is removed, as before. Without one it becomes `data.parquet` only if it decodes completely; otherwise it is renamed to `data.parquet.tmp-...damaged-<timestamp>` and stays. It is not written to `damaged_partitions.jsonl` (the run lock has no recorder); the warning in the log and `find -name '*.damaged-*'` are how it is found.
- **Run `verify-partitions` as the service user in production.** It writes `damaged_partitions.jsonl` and `partition_checks.json` in the working directory. Run with plain `sudo` it leaves them owned by root, and the daemon can then no longer write them. The daemon survives that (it logs an error, and still notes a checked month where it can), but records are lost. The command line is under "Checking a deploy".
- **`save` still trusts its caller; `merge` does not.** `save(request, new_data, existing_data)` replaces every month present in either frame with what the two frames hold, so an incomplete `existing_data` shrinks a month. That is its old behaviour, kept for the legacy migration (whose overwrite mode relies on it). The daemon goes through `merge`, which reads the stored rows from disk. Do not call `save` from the daemon path.
- **One failing ticker ends the cycle for the tickers after it.** `IntervalScheduler.run` has no per-ticker error handling, so an exception in one ticker (a damaged file in a touched month, an unexpected fetch result) stops the cycle there; the daemon starts the next cycle after its sleep and stops at the same ticker again. This predates Batch 3 and was not changed. The journal from 2026-09-04 to 2026-10-03 has no such failure (every traceback in it is from the midnight restart). With Step J an unreadable file in the current month costs one cycle: it is moved aside, and the next cycle writes a new file and goes on. A file with a schema problem or no rows is not moved, so it still fails the same ticker in every cycle until someone deals with it. Worth deciding with Step D, which makes each cycle's result matter more. **Decided 2026-10-10:** Step D changes this; a failing ticker is logged and skipped (Decision 6).
- **Step D activates a code path production has effectively never run.** Once `last_data_date` is saved, the fetch decision becomes `business_days_between(last_data_date, today) > 0`, which works in whole days. For 1-minute data on a 2-hour cycle this must be reviewed and covered by tests before deploying, so that saving the registry does not leave gaps. Yahoo serves only the last 7 days of 1-minute bars, so a ticker that goes unfetched for longer loses data permanently. Read from the code on 2026-10-10, not tested: `last_data_date` is stored as a date and `get_today()` is today at 17:00 host time (the host runs on UTC; Friday on a weekend), so the count is 0 for the rest of a day once a ticker has returned one bar of that day. The ticker is then skipped until the next business day, and the fetch changes from "the last 7 days" (`_fetch_all`) to a window from `last_data_date` to 17:00 (`_fetch_window`). The rest of a day's bars arrive with the next day's fetch, Friday's on Monday. No gap follows from that by itself, but each bar is requested once or twice instead of about thirty times, so a day that Yahoo serves incompletely on that one fetch stays incomplete. **Decided 2026-10-10 (Decision 6):** the path is not activated. The fetch stays "the last 7 days", `last_data_date` is not written, and a ticker is fetched once per night.
- **Step D also lets the not-found cycle advance.** Tickers will start reaching `permanently_dead` for the first time in the daemon. Check the first week's registry changes against expectations before trusting the pruning that follows. Measured on 2026-10-10: between 1,100 and 1,550 of the 9,277 tickers return no data in every cycle, so that many enter the cycle at once. The rule (`TickerRegistry.update_ticker_interval_status`, `is_active_for_interval`): "no data" on three different days starts a cooling period in which the ticker is not fetched for 7 business days; one retry follows, and if that returns nothing the interval is `permanently_dead`. **Decided 2026-10-10:** it does not. A cycle pauses nobody and marks nobody dead (Decision 6).
- **A Yahoo outage looks like "no data" for every ticker.** `_fetch_window` turns any exception into an empty result, and yfinance reports a throttled or failed request as "possibly delisted; no price data found". With the registry saved, three days of Yahoo refusing this host would put all tickers into cooling, and 7 business days without a fetch is longer than the 7 days Yahoo serves: a hole in every ticker that cannot be refilled. Today this cannot happen because the not-found state is thrown away at the start of each cycle. Step D needs a guard against it before the registry is saved. **Decided 2026-10-10:** with no automatic pause, an outage cannot stop a ticker from being asked; it costs the nights it lasts, and a hole appears only when it lasts longer than about six days. A failed request is told from "no data" with `raise_errors=True`; how cleanly that works under throttling is to be found out in D3.
- **The weekly ticker maintenance saves the registry from memory.** Since Batch 2 the daemon is no longer restarted every night, so for the first time it stays up for the 7 days after which maintenance runs again, with the last cycle's results in memory. The save would put `last_data_date` for every ticker with data into `tickers.json`, and a not-found streak of 1 for the 1,100 to 1,550 others. From then on the fetch is a window since the saved date; every ticker is still fetched in every weekday cycle, so no gap is expected. The not-found count would advance once a week: a pause from about 2026-10-25 and `permanently_dead` from about 2026-11-08, with no outage guard and no reviewed list. A restart before the maintenance is due prevents it. Step D ends the problem: it writes no `last_data_date` and sets no streak.
- **One fetch a night leaves 7 requests per bar, where there were about 30.** The measurement behind it is one Friday night (Progress Log, 2026-10-10): a second pass 5.5 hours after the first changed 14 of 7,744 files. A ticker whose request fails is asked again in the same night; a ticker missed for a whole night has six more.
- **The 22:00 UTC start is one hour after the winter close.** The measured night was in summer time, with the first cycle starting two hours after the close. If Yahoo completes a session's last bars later than that, they are stored the following night; nothing is lost, but check the first week of winter time (from 2026-11-02).
- **A revert of Step D refills only while `last_data_date` stays unwritten.** The previous release fetches the last 7 days for every ticker in every cycle because it finds no `last_data_date`. Step D must keep it that way (test in D5). The revert has to happen within 7 days of the first bad night.
- **A deploy by night interrupts the nightly cycle.** A stopped cycle is safe: the tickers not yet done are fetched after the restart. Deploying by day avoids it.
- **There is no backup of the collected data.** Stated by the owner on 2026-10-03. Every safeguard in this ADR assumes that a lost or damaged file is gone for good. A backup is a separate decision and is not part of this ADR.
- **Scoped writes must not drop rows.** A test must show that rows merged into a touched month are complete and that deduplication within that month still works.
- **The daemon exit skips Python's own cleanup.** After `exit_daemon_process`, no `atexit` handler and no `finally` block further up the stack runs. It is called only as the last statement of a daemon command, after the loop's own cleanup. Anything a daemon must do on shutdown has to happen before that call, not in an `atexit` handler. Tests replace the final `os._exit` (autouse fixture `daemon_exit_calls` in `tests/conftest.py`); without that, a daemon test that runs to a normal stop would end the test run.
- **A killed daemon now leaves its PID file behind.** Before, the directory was deleted on every stop, which also removed the PID file of a daemon that had been killed. Now the file stays, and the next start removes it after checking that no process has that PID. If the PID has meanwhile been given to another process of user `yfparqed`, the start is refused until the file is deleted by hand. PIDs are reused only after the counter wraps at about four million, so this is accepted.
- **A stop request now cuts work short at item boundaries.** Safe because every unit of work is atomic: a stored file is whole, an abandoned monthly consolidation deletes its temp file and leaves the old monthly file in place, and a partly fetched date stays `partial` and resumes.
- **A stopped month-end consolidation is not retried by anything of its own.** Daily files stay the record, so nothing is lost, but the monthly file is not written until the month is triggered again: by a later cycle whose API window (about three trading days) still lists dates of both months, or by hand with `xetra-parqed consolidate-month DETR --all`. A stop at the month boundary is the case to watch; a kill during consolidation before this step had the same effect. After a restart in the first days of a month, check that the previous month's `Consolidated to monthly` line appears. Work done up to the stop is thrown away (consolidation is not resumable), which costs about a minute of I/O at the next attempt.
- **Logrotate and loguru both rotate the Xetra log.** Loguru's `retention` glob also matches logrotate's `xetra-DETR.log.1` and `.log.N.gz`, so whichever policy is older deletes first; both keep 30 days, so the result is the same. Neither touches the other's live file.
- **`copytruncate` can lose a few log lines.** Lines written between logrotate's copy and its truncate are not in either file. Accepted for a log file; the alternative (rename and restart) is what killed a cycle every night. Do not add a `postrotate` that restarts or reloads the services.
- **Memory limits before Step A would do harm.** With the current code a limit makes the monthly consolidation fail every month. Step H depends on Step A.

## Alternatives Considered

**End the process from a wrapper around the CLI instead of from inside the daemon commands.** Raised in the code review of Step F and not done. Today `exit_daemon_process` is the last statement of each daemon command, so anything that runs a daemon command in-process (a test, a script) and lets it stop normally is ended by `os._exit(0)` unless the call is replaced, as the test suite does. The alternative is a small `main()` per console script that runs the Typer app and then calls `exit_daemon_process`; the commands would return normally, and the test fixture and the three `if daemon:` blocks would go away. It was left because it changes the entry points in `pyproject.toml` and has to carry "this was a daemon run that stopped normally" out of the command. Worth doing if a second in-process caller of the daemon commands ever appears.

**Fetch each ticker once per business day, as the code stood (Step D).** Once `last_data_date` is saved, the existing code skips a ticker for the rest of a day on which it returned one bar, and fetches a window since that date. Rejected: each bar is requested once or twice, so a day that Yahoo serves incompletely on that fetch stays incomplete, and Friday afternoon arrives on Monday.

**Skip a ticker only while the market has been closed since its last fetch (Step D).** The first proposal: 11 to 13 full cycles a week. Replaced by the nightly fetch: it needs a market calendar and time-zone handling, cycles during the session store the minute in progress, and for historical data nothing is gained by fetching during the session.

**Keep the not-found rule and guard it against outages (Step D).** Discard a cycle's not-found results when liquid tickers come back empty, and list who would be pruned before switching it on. Rejected: the guard recognises only the outages it was written for, and the 7-business-day pause is 9 to 11 calendar days, longer than Yahoo's 7, so a thin ticker that trades again during the pause loses bars even without an outage. With one fetch a night the pause would save about half an hour of requests.

**Make `add-ticker` and `remove-ticker` refuse while a cycle runs (Step D).** A few lines. Rejected in favour of the lock file with merge on save, so that the owner does not have to stop the daemon to add a ticker.

**Add RAM or move to an SSD.** Rejected as the fix: it hides the waste without removing it, and the work grows with every month of data and every ticker. Still worthwhile later for other reasons.

**Memory limits only.** Rejected: the consolidation would be killed each month and the monthly file would never be produced.

**Make `get_missing_dates` return only missing dates.** Considered (it is the alternative noted under item #3 of the 2026-05-01 ADR). It would narrow the trigger, but the current day must be rechecked every cycle anyway, and it does not make consolidation safe to call twice or bound its memory. It can be done in addition; it is not a substitute.

**Polars or DuckDB for the consolidation.** Both can stream. Rejected for now because PyArrow is already in the write path and one day at a time is enough; no new dependency in the daemon's hot path.

**Append each day to the monthly file as it completes.** Considered. Rejected for now: Parquet files cannot be appended in place, so this means rewriting the monthly file daily or keeping a writer open across restarts. Once-per-month streaming is simpler and cheap.

## Consequences

- Xetra consolidation runs once per month, in memory proportional to one trading day, and repeated triggers cost a few `stat` calls.
- A Yahoo cycle opens only the current month's partition per ticker and rewrites it only when it gained or changed a bar. After Step D there is one full cycle per night, about 3.5 hours from 22:00 UTC, where there were about 30 a week, and the host is free of Yahoo work during the day. Each bar is requested 7 times instead of about 30.
- The legacy single-file layout is frozen: it keeps working, and it gets no further optimisation.
- The daemons stop within seconds of a stop request and are no longer restarted nightly.
- Monthly Xetra files are explicitly not globally time-sorted. This matches what has been on disk since May 2026.
- A runaway daemon is contained by its own memory limit instead of pushing the whole machine into swap.
