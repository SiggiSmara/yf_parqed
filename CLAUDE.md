# CLAUDE.md — yf_parqed

Agent instructions for this codebase. Read this before doing anything else.

## Non-negotiable rules

- **Always run Python via `uv run`** — never `python`, `python3`, or direct venv activation. Example: `uv run pytest`, `uv run python3 -c "..."`, `uv run xetra-parqed ...`
- **Never edit `pyproject.toml` manually** — use `uv add` / `uv remove` for dependency changes
- **All tests must pass before finishing any task**: `uv run pytest` — currently 448 passed, 1 skipped
- **Data safety**: existing Parquet files are the primary record. Never delete or overwrite without explicit user confirmation. See `.github/DATA_SAFETY_STRATEGY.md` for the full ruleset.

## Project layout

```
src/yf_parqed/          # Main package
  xetra/                # Deutsche Börse Xetra pipeline
    xetra_parser.py     # JSON→DataFrame (multi-schema registry)
    xetra_service.py    # Orchestration: fetch / parse / store
    xetra_fetcher.py    # HTTP client with rate limiting
    exceptions.py       # XetraSchemaUnknownError
  common/               # Shared storage, config, path building
  xetra_cli.py          # Typer CLI entry point (xetra-parqed)
tests/                  # Pytest suite — mirrors src structure
docs/adr/               # Architecture Decision Records
  to-do/                # Agreed, not started
  in-progress/          # Active implementation
  implemented/          # Done
  idea/                 # Exploratory
  archived/             # Superseded
.github/                # Extended operational docs (read these for deep context)
```

## CLI entry points

| Command | Purpose |
|---|---|
| `xetra-parqed fetch-trades DETR --daemon` | Run Xetra collector daemon |
| `xetra-parqed list-files DETR` | List available files from Deutsche Börse API |
| `xetra-parqed check-status DETR` | API availability vs local storage |
| `xetra-parqed update-isin-mapping` | Refresh ISIN→ticker reference data |
| `xetra-parqed cleanup-raw-cache DETR` | Delete aged raw cache files once data is in Parquet |
| `xetra-parqed reprocess-raw-cache DETR DATE` | Rebuild a daily Parquet from raw cache after data loss |
| `yf-parqed` | Yahoo Finance data collector |
| `yf-parqed add-ticker TICKER` | Add or resurrect a ticker as manually managed (exempt from auto-pruning) |
| `yf-parqed remove-ticker TICKER` | Permanently deactivate a ticker; not reactivated by CSV updates |
| `uv run pytest` | Run full test suite |

## Production environment

The daemon runs as a systemd service under user `yfparqed`. **Do not confuse the dev repo with the production installation.**

| Location | Purpose |
|---|---|
| `/opt/yf_parqed/` | Production code installation |
| `/opt/yf_parqed/.venv/` | Production virtualenv |
| `/var/lib/yf_parqed/` | Working directory (config, data) |
| `/var/lib/yf_parqed/data/` | All collected Parquet data |
| `/var/log/yf_parqed/` | Service logs |
| `/run/yf_parqed/` | PID files |

Production data path pattern:
- Xetra trades: `/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=YYYY/month=MM/day=DD/trades.parquet`
- Xetra monthly: `/var/lib/yf_parqed/data/de/xetra/trades_monthly/venue=DETR/year=YYYY/month=MM/trades.parquet`
- Xetra raw cache: `/var/lib/yf_parqed/data/de/xetra/raw/DETR/year=YYYY/month=MM/day=DD/{filename}.json.gz` (7-day TTL)
- Yahoo Finance: `/var/lib/yf_parqed/data/us/yahoo/stocks_<interval>/ticker=<TICKER>/...`

When the CLI is invoked via systemd it uses `--wrk-dir /var/lib/yf_parqed`. In dev, the default root is a relative `data/` from the working directory.

**Deployment:** Use `sudo ./daemon-manage.sh update` — it pulls code, syncs dependencies, clears bytecache, runs any pending Parquet column migrations, runs any pending ticker registry pruning, then restarts all enabled services. Do not deploy manually.

**Operational tools** (`tools/` directory):

| Script | Purpose |
|---|---|
| `tools/prune_registry.py` | Mark old-schema globally-not-found and dead-instrument-suffix tickers as `permanently_dead` so the normal pruning cycle can remove them. Dry-run by default; `--apply` to write. Invoked automatically by `daemon-manage.sh update`. |

## Xetra parser — current state (as of 2026-05-01)

The parser supports two schemas:

| Version | Sentinel field | Era |
|---|---|---|
| `2025-legacy` | `isin` present in JSON | Oct 2025 – Feb 2026 |
| `2026-mifid` | `instrumentIdentificationCode` present in JSON | Mar 2026 onward |

Both schemas produce a DataFrame with MiFIR column names: `isin`, `price`, `quantity`, `price_currency`, `trading_date_time`, `execution_venue`, `transaction_id`, plus `schema_version`. All on-disk Parquet files use these column names — legacy files were migrated in May 2026.

If the parser encounters an unknown schema it raises `XetraSchemaUnknownError`. The raw `.json.gz` bytes are already in the raw cache before parsing is attempted, so no data is lost.

## ADR process

Active decisions live in `docs/adr/`. Check `docs/adr/ADR_INDEX.md` for current state. Before implementing anything structural, check whether an ADR already covers it. In-progress ADRs define the sequenced steps to follow.

## Key reference docs

| File | When to read it |
|---|---|
| `.github/ARCHITECTURE.md` | Understanding the overall system design |
| `.github/DATA_SAFETY_STRATEGY.md` | Before touching any storage code |
| `.github/DEVELOPMENT_GUIDE.md` | Adding services, CLI commands, storage changes |
| `.github/TESTING_GUIDE.md` | Test patterns, fixtures, coverage goals |
| `.github/STORAGE_STRUCTURE.md` | Partition layout, DuckDB query patterns |
| `docs/daemon/INSTALLATION.md` | Production deployment and systemd config |
