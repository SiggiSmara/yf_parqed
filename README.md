# yf-parqed

Persistent storage of yfinance ticker data in parquet based on [ranaroussi's](https://github.com/ranaroussi) [yfinace](https://ranaroussi.github.io/yfinance/index.html). Primary use case for using this package is collecting data for back testing or historical analysis. If the update interval is less than one day (24 h) this package will skip updating.

It uses rate limiting on the calls to the Yahoo APIs (can be controlled) so as not to trigger any
usage limits.

It can download the actual lists of Nasdaq and Nyse tickers from [datahub.io](https://datahub.io/) ( specifically  [nasdaq-listed.csv](https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv) and [nye-listed.csv](https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv) ) and download historical ticker prices for each ticker found in there via yfinance.

Along with which tickers to sync locally you can also control the time interval(s) you are interested in.
Each interval has their own folder and in there each ticker will have their own parquet.

In addition, if any ticker returns no data or you are no longer interested in syncing data for it you have the option of adding them to an exclude list to simplify the management of the list of tickers to download.

## Partition-aware storage

For large datasets you can switch from the legacy `stocks_<interval>/<ticker>.parquet` layout to Hive-style partitions under `data/<market>/<source>/stocks_<interval>/ticker=<TICKER>/...`. The migration flow is opt-in and maintains backward compatibility until you complete it.

Note: Partition-Aware storage is implemented and shipped (2025-10-19). See `docs/adr/2025-10-12-partition-aware-storage.md` and `docs/release-notes.md` for rollout and operational guidance.

1. Move your existing legacy folders **under** `data/legacy/` (for example `mv stocks_1d data/legacy/`). The migration CLI refuses to run until the legacy tree lives there.
2. Create a migration plan with the Typer helper:

   ```bash
   yf-parqed-migrate init --venue us:yahoo --interval 1m --interval 1h --interval 1d
   ```

   Adjust the venue/interval list to match the data you want to migrate.
3. Run the migration:

   ```bash
   yf-parqed-migrate migrate --all
   ```

   The command estimates disk requirements, copies each ticker into the partitioned layout, and verifies row counts + checksums.
4. After a venue completes, the runtime automatically uses the partitioned backend. You can still override behavior manually with `yf-parqed partition-toggle` (see CLI notes below).

Legacy-only workflows continue to function. You can migrate venues one at a time and mix legacy + partitioned sources in the same workspace.

## Migration CLI (fast mode and flags)

The repository includes a Typer-based CLI helper `yf-parqed-migrate` to create a migration plan and move legacy per-ticker parquet files into the partitioned, Hive-style layout. During development we added several operational levers to tune performance vs durability. This section summarizes the most-used options and the safe defaults.

Key flags

- `--fast` — A convenience preset that enables faster migration defaults: `--overwrite-existing`, `--no-fsync`, and `--row-group-size=65536`. Use this for large batch migrations when you can tolerate re-checking data with `verify` afterwards. This flag still keeps checksum verification enabled.

- `--overwrite-existing` — Destructive. Deletes the target interval partition folder before writing. Use when you want a clean copy and don't need to preserve existing partition files.

- `--no-fsync` — Disables calling `fsync()` on temporary partition files before the atomic rename. This speeds up writes but reduces immediate durability: data may still be in OS buffers until the kernel flushes to disk.

- `--row-group-size <N>` — When provided, uses pyarrow to write parquet files with the specified row group size. Large values (e.g. 65536) can increase write throughput and reduce CPU overhead in many cases.

- `--compression <gzip|snappy|none>` — Optional compression codec for partition parquet files. The special value `none` disables compression. The `--fast` preset defaults to gzip unless you explicitly pass a codec.

Verification

- `yf-parqed-migrate verify <venue> <interval>` — Compares legacy vs partitioned data for each ticker using row counts and SHA256 checksums. Run this after using `--fast` if you disabled fsync or overwrote existing data.

Safety notes

- `--fast` bundles settings that trade durability for speed. It intentionally leaves checksum verification enabled; however, combining `--fast` with `--overwrite-existing` is destructive. Always run `verify` after a large or destructive migration when using `--fast`.

- `--no-fsync` improves throughput but increases risk during power-loss or kernel crashes. Use on fast SSDs or in environments with reliable power if possible.

- The migration CLI will refuse to run unless your legacy files live under `data/legacy/` to avoid accidental cross-directory deletes.

Examples

- Create a plan and run a safe (default) migration:

```bash
yf-parqed-migrate init --venue us:yahoo --interval 1m --interval 1h
yf-parqed-migrate migrate --base-dir /path/to/workspace
```

- Fast migration (destructive overwrite, faster writes):

```bash
yf-parqed-migrate migrate --base-dir /path/to/workspace --fast --max-tickers 500
# then verify
yf-parqed-migrate verify us:yahoo 1m --base-dir /path/to/workspace --max-tickers 500
```

This README entry documents the operational knobs added to the migration flow. If you prefer the `--compression` option removed from the CLI entirely, I can remove it and update the tests accordingly; currently both the preset and explicit `--compression` stay supported for backward compatibility.

## How to install

At some point I might publish this to PyPI but until then simply clone the repo and use pip or your favorite package management tool to install the package: `pip install .`

## How to use

Still in flux, but generally:

1. Initialize the list of tickers to sync via `yf-parqed initialize`
2. Adjust the content of the `intervals,json`, `current_tickers.json` and `not_found_tickers.json`:
   1. intervals.json contains a list of the yfinance intervals you want to download
   2. current_ticker.json contains the list of tickers you want to download
   3. not_found_tickers.json contains a list of tickers that should be excluded
3. Trigger the initial snapshot via `uv-parqed update` with the `--start-date`and `--end-date` parameters set.
4. Any time after that you can run `uv-parqed update` without parameters to add new data to your local snapshot.

### Partition mode toggles

Use `yf-parqed partition-toggle` to control the storage backend once the migration metadata exists. Examples:

- `yf-parqed partition-toggle` → enable partition mode globally.
- `yf-parqed partition-toggle --market US --disable` → keep US venues on the legacy backend.
- `yf-parqed partition-toggle --market US --source yahoo --clear` → remove an explicit override so the venue follows the default rules.

The command updates `storage_config.json`; manual edits are rarely necessary now.

### Notes on `update`

 `uv-parqed update`  will detect if any tickers are not returning data and asks you via a prompt if you want to save them to the exclude list. You have two CLI options that allow you to control that behavior in the case when you are running this command via scripts:

- `--save-not-founds` will circumvent the prompt and save them to the exclude list
- `--non-interactive` will circumvent the prompt in the case when the `--save-not-found` is not present (resulting in the exclude list not being updated)

The current list of tickers from Nasdaq and Nyse  (> 9000 tickers in total) with the default limiter settings will take a considerable time given that the default limiter settings is no more than 2 API calls in a 5 second period.  You can of course play around with those settings, but they are coming from the [documentation of yfinance](https://ranaroussi.github.io/yfinance/advanced/caching.html) and they are very stable in my experience. Feel free to experiment with other values.

As the Yahoo finance APIs are rate limited (and not volume) it makes less sense (to me at least) to use an API cache mechanism, although that is easy to set up as well (see the above link to the yfinance documentation).

## About the package

The repo uses [uv](https://docs.astral.sh/uv/) and [pre-commit-uv](https://pypi.org/project/pre-commit-uv/).  

Make sure that uv is installed and then execute `uv tool install pre-commit --with pre-commit-uv --force-reinstall` in the repo.

The package is created with the [typer module](https://typer.tiangolo.com/) from [tiangolo](https://github.com/tiangolo),
so you can always add `--help` at the end of your cli command to get more information about options
and functionalities.

Logging is taken care of via loguru, and with the `--log-level` option you have access to set the level of logging detail.  Default logging level is `INFO`, for more verbose output set it to `DEBUG` and for less you can set it to `WARNING` or higher.
