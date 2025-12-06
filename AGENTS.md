# yf_parqed Technical Overview

## Purpose

`yf_parqed` is a Python package for persistent, interval-aware storage and management of stock ticker data retrieved from Yahoo Finance. It is designed for robust, scalable, and auditable financial data collection, supporting both historical and live updates.

## Core Features

- **Unified Ticker Management**: All tickers are stored in a single `tickers.json` file, with metadata and interval-specific status tracking.
- **Interval-Aware Data**: Each ticker tracks its status and last data availability per interval (e.g., `1d`, `1h`).
- **Status Lifecycle**:
  - `active`: Ticker is valid and being traded (with per-interval last checked/found dates).
  - `not_found`: Ticker is globally excluded (e.g., delisted or never traded), with interval-specific not found tracking.
- **Data Storage**: Price and OHLCV data for each ticker/interval is stored in Parquet files under `stocks_<interval>/` directories.
- **Migration Support**: Includes scripts to migrate legacy JSON formats to the new unified, interval-aware format.
- **CLI and Automation**: Typer-based CLI for initialization, updating tickers, updating data, and confirming/reparsing not found tickers.
- **Testing**: Pytest-based test suite validates read/write logic, interval status management, and data integrity.
````markdown
# yf_parqed Technical Overview — Pointer

This repository's operational policies, runbooks, and safety guidance live in the `.github/` directory and the `docs/` tree. The fuller historical and agent-oriented summary previously in this file has been migrated into `.github/AGENTS_SUMMARY.md`.

Canonical docs (operator & contributor guidance):

- Data safety, migration rules and runbooks: `.github/DATA_SAFETY_STRATEGY.md`
- Development & build workflow: `.github/DEVELOPMENT_GUIDE.md`
- Testing guide and patterns: `.github/TESTING_GUIDE.md`
- Architecture and service specs: `ARCHITECTURE.md`
- Release notes & roadmap: `docs/release-notes.md`, `docs/roadmap.md`

For the historical refactor notes and the automated coverage map that used to be in `AGENTS.md`, see `.github/AGENTS_SUMMARY.md`.

````
        "last_data_date": "2024-01-19",
