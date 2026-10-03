# ADR 2026-10-03: Daemon Resource Footprint (Memory, Disk I/O, Shutdown)

## Status: In Progress

**Batch 1 is deployed (2026-10-03), ahead of its 2026-11-01 deadline.** The first real test is the month change on 2026-11-01 to 11-04; see "Checking a deploy". Batch 2 (Steps E, F, G) is implemented and waiting for deploy.

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

- **Write only the months the new data touches.** `PartitionedStorageBackend.save` writes only partitions whose month appears in `new_data`. Untouched months are left alone. With a 7-day fetch that is one month per ticker, two around a month boundary, instead of all of them.
- **Then read only those months.** Once writes are scoped, restrict the read to the same months, so a cycle no longer opens every partition of every ticker.
- **Finally, persist the registry.** In daemon mode, save `tickers.json` at the end of every cycle regardless of `--save-not-founds`, and also every N tickers during a cycle so that a crash loses minutes, not hours. The save must be atomic (temp file and rename).

### 3. Make shutdown prompt and stop the nightly restart

- **Check the flag inside the loops.** The Yahoo per-ticker loop and the Xetra per-date and per-file loops check the shutdown flag between items. An atomic write in progress is always allowed to finish.
- **End the daemon process without interpreter teardown.** When a daemon loop has ended and its own cleanup is done (HTTP client closed, run lock released, PID file removed), the process writes out its queued log messages, closes its log files and ends with `os._exit`. The kernel then frees the memory without reading it back from swap. The helper is `exit_daemon_process` in `common/process_exit.py`; the Xetra trade daemon, the ISIN mapping daemon and the Yahoo daemon all use it. One-shot commands and error exits are unchanged.
- **Keep the shared runtime directory.** Both unit templates set `RuntimeDirectoryPreserve=yes`, so stopping one service no longer deletes `/run/yf_parqed` and the other service's PID file with it.
- **Check for a stop request every 10 seconds while idle.** A signal does not cut `time.sleep` short, so a daemon notices a stop only when its current sleep ends. All idle waits (between cycles, outside active hours, the ISIN daemon's wait until 00:05) now sleep in 10-second pieces. The outside-hours waits and the ISIN daemon used 60-second pieces before.
- **Stop restarting from logrotate.** Use `copytruncate` for the Xetra log file and remove the `postrotate` restart. The templates live in `daemon-manage.sh` and `docs/daemon/INSTALLATION.md`.

### 4. Add systemd memory limits

Add `MemoryHigh`, `MemoryMax` and `MemorySwapMax` to both unit templates so that a future regression is contained to the service that caused it. `.github/ARCHITECTURE.md` already recommends this; the templates never got it. Choose the values from the peaks observed after Batches 1 to 3 are deployed.

## Sequenced Steps

Steps are grouped into batches. A batch is one working session, one commit series and one deploy. Batches run in order, one at a time; see "Work Plan and Handoff" for why and for who does what.

**Batch 1 — Xetra consolidation**

- [x] **Step A** — *(implemented and deployed 2026-10-03)* Monthly consolidation: skip-when-current fingerprint, streaming `ParquetWriter`, schema unification, leftover mini-files merged first, unreadable days left out and recorded, never shrink an existing monthly file, dead `time` sort removed. Daily merge: watermark that separates stale mini-files from new ones. Raw-cache cleanup: per-day proof. Tests in `tests/test_xetra_consolidation.py` and `tests/test_xetra_raw_cache.py`. **Must be deployed before 2026-11-01.**
- [x] **Step A2** — *(done 2026-10-03)* One-off repair of the monthly files that are behind their daily data (January, February, May, June 2026). Runbook under "Repairing the monthly files" below. Needs Step A deployed first; run by the owner.

**Batch 2 — Shutdown and logrotate**

- [x] **Step E** — *(implemented 2026-10-03, not yet deployed)* In-loop shutdown checks for both daemons. A `should_stop` callable (`common/shutdown.py`) is passed down: `IntervalScheduler.run` checks it before each ticker, `XetraService.fetch_and_store_missing_trades_incremental` before each date and file, and `_consolidate_to_monthly` between days (an abandoned consolidation deletes its temp file; see Risk Controls for when it is retried). The fetcher's burst cooldown and 429 retry waits sleep in 1-second steps and raise `ShutdownRequested`, which the file loop treats as a stop, not a failed file. A stopped Xetra cycle skips the end-of-cycle consolidation. All three signal handlers now only record the signal; the main loop logs `Received signal N, shutting down gracefully...`. Tests in `tests/test_shutdown_in_loops.py`. Not covered: Yahoo ticker maintenance (`--ticker-maintenance`, weekly by default) and a single running download or day merge.
- [x] **Step F** — *(implemented 2026-10-03, not yet deployed)* Diagnosed: interpreter teardown reading the swapped-out process back from disk; see "What the exit hang turned out to be". Fixed with `exit_daemon_process` (`common/process_exit.py`) at the end of all three daemon loops, and `RuntimeDirectoryPreserve=yes` in the unit templates (`daemon-manage.sh`, `docs/daemon/INSTALLATION.md`). Tests in `tests/test_process_exit.py` and one stop-request test per daemon. After a code review the same day: every idle wait in the three daemon loops now checks for a stop request every 10 seconds (some checked every 60, which is longer than the Xetra units' 30-second stop timeout).
- [x] **Step G** — *(implemented 2026-10-03, not yet deployed)* Logrotate: `copytruncate`, no restart, in `daemon-manage.sh`, `docs/daemon/INSTALLATION.md` and `docs/DAEMON_MODE.md`. `daemon-manage.sh update` did not touch the logrotate config before, so it now replaces an installed config that still contains `postrotate` (function `refresh_stale_logrotate`); no prompt, because the file is generated and carries no local settings.

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

The agent cannot deploy. Production (`/opt/yf_parqed`) is a checkout of `main`, and `sudo ./daemon-manage.sh update` runs `git pull origin main` before restarting the services. A commit on `develop` therefore reaches production only after it has been pushed **and merged into `main`**. Each batch ends with commits on `develop`; the owner pushes, merges to `main` and runs the update. Record the deploy date in the Progress Log.

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
- **Batch 3, after B and C.** Pick a ticker and list its partition files: only the current month's file should have a fresh modification time after a cycle. Cycle duration (from `Processing ... tickers` to `All tickers were processed.` in `journalctl -u yf-parqed`) should fall well below the 4h08m baseline.
- **Batch 3, after D.** For a week, repeat the gap check below and compare with the baseline. Also confirm that `tickers.json` now carries `last_data_date` values and that the list of tickers going `permanently_dead` looks reasonable.

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

## Risk Controls

- **Daily files stay the record.** Consolidation never deletes or modifies daily files. Before deploying Step A, run the new consolidation in dev against a copy of a real month and compare row count and per-ISIN counts with the existing monthly file.
- **Raw-cache cleanup became stricter, not looser.** It deletes a raw file only when that specific day is proven to be in a readable daily file or listed in a monthly file. Until Step A2 has rebuilt the older monthly files, they carry no day list and prove nothing; this only matters for days without a daily file, and currently every day has one except the two being repaired.
- **The daily merge now keeps data it used to delete.** Mini-files found next to an existing daily file are merged when they were staged after it. If the system clock is set backwards between a crash and the next run, an already-merged mini-file could be merged twice. That needs a crash and a clock step together; it is accepted.
- **Step D activates a code path production has effectively never run.** Once `last_data_date` is saved, the fetch decision becomes `business_days_between(last_data_date, today) > 0`, which works in whole days. For 1-minute data on a 2-hour cycle this must be reviewed and covered by tests before deploying, so that saving the registry does not leave gaps. Yahoo serves only the last 7 days of 1-minute bars, so a ticker that goes unfetched for longer loses data permanently.
- **Step D also lets the not-found cycle advance.** Tickers will start reaching `permanently_dead` for the first time in the daemon. Check the first week's registry changes against expectations before trusting the pruning that follows.
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
