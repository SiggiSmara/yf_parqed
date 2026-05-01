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
| `yf-parqed` | Yahoo Finance data collector |
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
- Yahoo Finance: `/var/lib/yf_parqed/data/us/yahoo/stocks_<interval>/ticker=<TICKER>/...`

When the CLI is invoked via systemd it uses `--wrk-dir /var/lib/yf_parqed`. In dev, the default root is a relative `data/` from the working directory.

## Xetra parser — current state (as of 2026-05-01)

The parser supports two schemas:

| Version | Sentinel field | Era |
|---|---|---|
| `2025-legacy` | `isin` present in JSON | Oct 2025 – Feb 2026 |
| `2026-mifid` | `instrumentIdentificationCode` present in JSON | Mar 2026 onward |

Both schemas produce a DataFrame with MiFIR column names: `isin`, `price`, `quantity`, `price_currency`, `trading_date_time`, `execution_venue`, `transaction_id`, plus `schema_version`.

Stored data from 2025–early 2026 uses the **old column names** (`volume`, `currency`, `trade_time`, `venue`, `trans_id`). A migration to the new names is pending before consistent cross-era querying works.

If the parser encounters an unknown schema it raises `XetraSchemaUnknownError` and the service writes the raw `.json.gz` file to `{wrk_dir}/data/de/xetra/quarantine/{venue}/` before re-raising.

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
