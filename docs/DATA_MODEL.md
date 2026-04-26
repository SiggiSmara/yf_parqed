# Data Model Reference for Analytics

This document describes the data collected by `yf_parqed` in terms of **what is stored, where it lives, and the underlying principles** that govern the storage design. It is written for agents and engineers building analytics workflows in a separate repository that consumes this data using DuckDB and Python data science tooling.

---

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Production Data Location](#production-data-location)
3. [Root Layout](#root-layout)
4. [Dataset: Yahoo Finance OHLCV](#dataset-yahoo-finance-ohlcv)
5. [Dataset: Xetra Raw Trades (Phase 1 — current)](#dataset-xetra-raw-trades-phase-1--current)
6. [Dataset: Xetra Normalized Trades (Phase 2a — planned)](#dataset-xetra-normalized-trades-phase-2a--planned)
7. [Dataset: Xetra OHLCV (Phase 2b — planned)](#dataset-xetra-ohlcv-phase-2b--planned)
8. [Temporal Model and Timezones](#temporal-model-and-timezones)
9. [Identifier Model](#identifier-model)
10. [Data Quality and Caveats](#data-quality-and-caveats)
11. [Data Completeness Monitoring](#data-completeness-monitoring)
12. [DuckDB Query Patterns](#duckdb-query-patterns)
13. [Cross-Source Analytics](#cross-source-analytics)
14. [Storage Sizing Reference](#storage-sizing-reference)

---

## Design Philosophy

### Hive-Style Partitioning

All data is stored using **Hive-style directory partitioning**: each partition dimension is a `key=value` path segment. This is the same convention used by Apache Spark, Hive, and natively supported by DuckDB (`hive_partitioning=1`). The partition keys are embedded in the path, not in the file itself, so DuckDB can use them for partition pruning without reading any parquet data.

```
data/<market>/<source>/<dataset>/key1=val1/key2=val2/.../filename.parquet
```

### Single File Per Leaf Partition

Each leaf partition contains exactly one file (`data.parquet` for OHLCV, `trades.parquet` for raw trades). Updates are performed as read → merge → deduplicate → atomic write. There are no delta or append files.

### Market and Source Hierarchy

The top two levels separate data by **market** (country code) and **source** (provider):

| Market | Source | Description |
|--------|--------|-------------|
| `us`   | `yahoo` | Yahoo Finance — US-listed equities |
| `de`   | `xetra` | Deutsche Börse Xetra — German-listed equities |

This hierarchy makes it safe to query across markets without ambiguity, and allows future markets/sources to be added without restructuring.

### Monthly Granularity

Data is partitioned by **month**, not day or year. This is a deliberate balance:
- **Too coarse (year)**: Large files, expensive to update a single day of data.
- **Too fine (day)**: Thousands of tiny files, high DuckDB planning overhead for range queries.
- **Month**: ~20–400 trading sessions per file depending on interval, efficient both for updates and for analytical range scans spanning multiple months.

### Timestamps in UTC

All timestamps are stored **timezone-naive UTC**. Market-local time (CET/CEST for Xetra, ET for US equities) is never stored directly. UTC boundaries must be computed when filtering by local trading hours (see [Temporal Model](#temporal-model-and-timezones)).

---

## Production Data Location

The collection daemons run under the system user `yfparqed` and write to a fixed system path. When connecting an analytics repo to the live data, use the absolute path:

```
/var/lib/yf_parqed/data/
```

This is the `DATA_ROOT` for all path patterns in this document. All subdirectories (`us/`, `de/`, `legacy/`) live here.

```python
# Canonical DATA_ROOT for production
DATA_ROOT = '/var/lib/yf_parqed/data'
```

> For local development or testing with a copy of the data, substitute the path to your local `data/` directory.

---

## Root Layout

```
data/
├── us/
│   └── yahoo/
│       ├── stocks_1m/          Yahoo Finance 1-minute OHLCV
│       ├── stocks_1h/          Yahoo Finance 1-hour OHLCV
│       └── stocks_1d/          Yahoo Finance 1-day OHLCV
│
├── de/
│   └── xetra/
│       ├── trades/             Raw per-trade tick data (venue × day partitions) [CURRENT]
│       ├── trades_monthly/     Consolidated monthly trade data (venue × month)  [CURRENT]
│       ├── trades_by_isin/     Normalized trades (venue × ISIN × month)         [PHASE 2a]
│       ├── stocks_1m/          Xetra aggregated 1-minute OHLCV                  [PHASE 2b]
│       ├── stocks_1h/          Xetra aggregated 1-hour OHLCV                    [PHASE 2b]
│       └── stocks_1d/          Xetra aggregated 1-day OHLCV                     [PHASE 2b]
│
└── legacy/                     Pre-migration flat files (deprecated, do not use)
    ├── stocks_1m/
    ├── stocks_1h/
    └── stocks_1d/
```

> **Analytics note**: Ignore `data/legacy/`. It is a migration artefact from before Hive partitioning was introduced. All current data lives under `data/us/` and `data/de/`.

---

## Dataset: Yahoo Finance OHLCV

**Status**: Live and collecting.

### Path Pattern

```
data/us/yahoo/stocks_<interval>/ticker=<TICKER>/year=<YYYY>/month=<MM>/data.parquet
```

### Intervals

| Dataset dir | Interval | Rows per trading day | Typical file size |
|-------------|----------|----------------------|-------------------|
| `stocks_1d` | 1 day    | 1                    | ~1 KB/month       |
| `stocks_1h` | 1 hour   | 6–7                  | ~15 KB/month      |
| `stocks_1m` | 1 minute | ~390                 | ~400 KB/month     |

### Parquet Schema

| Column         | Type              | Notes |
|----------------|-------------------|-------|
| `date`         | `datetime64[ns]`  | UTC timestamp; for `1d` this is midnight UTC of the trading date |
| `open`         | `float64`         | Opening price in USD |
| `high`         | `float64`         | Session high in USD |
| `low`          | `float64`         | Session low in USD |
| `close`        | `float64`         | Closing price in USD |
| `volume`       | `int64`           | Number of shares traded |
| `dividends`    | `float64`         | Dividend per share (0.0 on non-dividend dates) |
| `stock_splits` | `float64`         | Split factor (1.0 on non-split dates) |

### Partition Keys Available to DuckDB

When querying with `hive_partitioning=1`, DuckDB exposes these additional columns derived from the path:

| Column   | Example value | Description |
|----------|---------------|-------------|
| `ticker` | `AAPL`        | Stock ticker symbol |
| `year`   | `2025`        | Four-digit year |
| `month`  | `12`          | Two-digit month (integer after parsing) |

### Trading Session

US equities trade on the NYSE/NASDAQ regular session:
- **09:30–16:00 ET** (Eastern Time, UTC-5 in winter / UTC-4 in summer)
- **390 minutes** per full trading day — this is the theoretical maximum for 1m data
- Extended hours (pre-market and after-hours) are **not collected** by default

### Key Facts

- **Ticker symbols** are Yahoo Finance symbols (e.g. `AAPL`, `MSFT`, `BRK-B`). They are NOT ISINs.
- `1m` data from Yahoo Finance has a **7-day availability window** — older minutes are permanently inaccessible from the API.
- `dividends` and `stock_splits` are set to `0.0` / `1.0` respectively on normal trading days. Filter `dividends > 0` to find ex-dividend dates.
- Prices are **not adjusted** for splits or dividends at rest. Yahoo's `auto_adjust` is disabled to preserve raw prices. Apply adjustment logic in your analytics layer.
- The daemon runs an update loop every **~2 hours** by default — expect up to a 2-hour lag between market activity and data being written to disk.

---

## Dataset: Xetra Raw Trades (Phase 1 — current)

**Status**: Live and collecting.

Xetra publishes a 15-minute delayed stream of all on-exchange trades. `yf_parqed` downloads and stores the raw tick-level data before it expires (24-hour window).

### Path Pattern

```
data/de/xetra/trades/venue=<VENUE>/year=<YYYY>/month=<MM>/day=<DD>/trades.parquet
```

### Known Venues

| Venue code | Description |
|------------|-------------|
| `DETR`     | Xetra (primary electronic exchange) — highest volume |
| `DFRA`     | Frankfurt floor exchange |
| `DGAT`     | Xetra BEST (retail execution) |

### Parquet Schema

| Column              | Type              | Notes |
|---------------------|-------------------|-------|
| `message_id`        | `string`          | Unique message identifier |
| `source_name`       | `string`          | Source system name |
| `isin`              | `string`          | 12-character ISIN (e.g. `DE0007100000`) |
| `instrument_id`     | `string`          | Exchange instrument identifier |
| `trans_id`          | `string`          | Transaction ID |
| `tick_id`           | `int64` (nullable)| Tick sequence number |
| `price`             | `float64`         | Trade price in EUR |
| `volume`            | `float64`         | Number of shares (stored as float; cast to int for analysis) |
| `currency`          | `string`          | Always `EUR` for Xetra |
| `quote_type`        | `string`          | Quotation type |
| `trade_time`        | `datetime64[ns]`  | Trade execution timestamp, **timezone-naive UTC** |
| `distribution_time` | `datetime64[ns]`  | Data publication timestamp, **timezone-naive UTC** |
| `venue`             | `string`          | Trading venue code (redundant with partition but present in data) |
| `tick_action`       | `string`          | MiFID II tick action indicator |
| `instrument_code`   | `string`          | MiFID II instrument code |
| `market_mechanism`  | `string`          | MiFID II market mechanism |
| `trading_mode`      | `string`          | MiFID II trading mode |
| `negotiated_flag`   | `string`          | MiFID II negotiated transaction flag |
| `modification_flag` | `string`          | MiFID II modification indicator |
| `benchmark_flag`    | `string`          | MiFID II benchmark reference flag |
| `pub_deferral`      | `string`          | MiFID II publication deferral reason |
| `algo_indicator`    | `bool`            | `True` if trade was executed by an algorithm (`H` in source) |

### Partition Keys Available to DuckDB

| Column  | Example | Description |
|---------|---------|-------------|
| `venue` | `DETR`  | Trading venue code |
| `year`  | `2025`  | Four-digit year |
| `month` | `12`    | Two-digit month |
| `day`   | `05`    | Two-digit day |

### Trading Session and Daemon Active Window

The Xetra daemon is configured with `--active-hours "08:00-18:00"` (CET/CEST, 600 minutes) as its polling window. This is deliberately wider than the official trading session:

| Window | CET hours | Minutes | Purpose |
|--------|-----------|---------|---------|
| Daemon active | 08:00–18:00 | 600 | File polling window |
| Official session | 09:00–17:30 | 510 | Continuous trading + auctions |
| OHLCV filter (Phase 2b) | 09:00–17:30 | 510 | Bars include only this range |

The 600-minute window is used as the **theoretical maximum** when computing capture rate in monitoring queries.

### Key Facts

- **~535,000 trades/day** across all ISINs for DETR (single venue), with ~4,200 distinct ISINs active per month.
- Trade distribution is **highly skewed**: top 1% of ISINs (~42 of 4,200) account for ~60% of trade volume and file sizes.
- Files are ~23 MB/day uncompressed; ~50 MB/day raw Parquet at venue level.
- The `venue` column inside the file is redundant with the `venue=` partition directory but is present in every row.
- Optional MiFID II fields (`negotiated_flag`, `modification_flag`, etc.) may be `null` in some records — the schema is padded with `None` to keep it stable across files.
- **Do not query this dataset for single-ISIN analytics** — each file contains all ISINs mixed, so DuckDB must scan the full file (~50 MB) even for one ISIN. Use `trades_by_isin/` once Phase 2a is complete.

### Monthly Consolidation

Daily files are periodically consolidated into:
```
data/de/xetra/trades_monthly/venue=<VENUE>/year=<YYYY>/month=<MM>/trades.parquet
```
This has identical schema and is more efficient for month-level queries. Daily files are retained alongside consolidated files; prefer `trades_monthly/` for retrospective analysis.

---

## Dataset: Xetra Normalized Trades (Phase 2a — planned)

**Status**: Designed, not yet implemented. See `docs/adr/to-do/2025-12-05-ohlcv-aggregation-service.md`.

### Path Pattern

```
data/de/xetra/trades_by_isin/venue=<VENUE>/isin=<ISIN>/year=<YYYY>/month=<MM>/trades.parquet
```

### Purpose

Re-partitions the raw `trades/` data from a venue-first layout (all ISINs in one daily file) into an ISIN-first monthly layout. **Schema is identical to raw trades** — no aggregation, no column removal.

The transformation is:
```
trades/venue=DETR/year=2025/month=12/day=01..31/trades.parquet   (all ISINs mixed)
    → trades_by_isin/venue=DETR/isin=DE0007100000/year=2025/month=12/trades.parquet
    → trades_by_isin/venue=DETR/isin=DE0005190003/year=2025/month=12/trades.parquet
    → ... (~4,200 files for DETR per month)
```

### Analytics Impact

Before Phase 2a, filtering a single ISIN from a full month requires scanning ~690 MB. After Phase 2a, DuckDB prunes to the single-ISIN file (~115 KB average, ~5–20 MB for highly liquid names). Expected speedup: **10–50× for single-ISIN queries**.

### Partition Keys Available to DuckDB

| Column  | Example          | Description |
|---------|------------------|-------------|
| `venue` | `DETR`           | Trading venue |
| `isin`  | `DE0007100000`   | 12-character ISIN |
| `year`  | `2025`           | Four-digit year |
| `month` | `12`             | Two-digit month |

---

## Dataset: Xetra OHLCV (Phase 2b — planned)

**Status**: Designed, not yet implemented. Depends on Phase 2a.

### Path Pattern

```
data/de/xetra/stocks_<interval>/ticker=<ISIN>/year=<YYYY>/month=<MM>/data.parquet
```

Note: the partition directory is named `ticker=` (not `isin=`) to match the Yahoo Finance convention, enabling unified cross-source queries. The values stored there are ISINs (e.g. `DE0007100000`) until a future ISIN→ticker mapping layer is added.

### Parquet Schema (planned)

| Column           | Type             | Notes |
|------------------|------------------|-------|
| `date`           | `datetime64[ns]` | Bar open timestamp, UTC |
| `open`           | `float64`        | Opening price in EUR |
| `high`           | `float64`        | Session high in EUR |
| `low`            | `float64`        | Session low in EUR |
| `close`          | `float64`        | Closing price in EUR |
| `volume`         | `int64`          | Shares traded in bar |
| `isin`           | `string`         | Source ISIN for reference |
| `source_interval`| `string`         | `"tick"` (aggregated from raw trades) |
| `aggregated_at`  | `datetime64[ns]` | When aggregation was computed |

### Trading Hours Filter Applied

Only trades within the **official Xetra continuous trading session** are included in OHLCV bars:
- **09:00–17:30 CET/CEST** (pre-market and post-trade filings excluded)
- This captures ~99% of trade volume (see ADR for empirical breakdown)
- DST-aware: UTC boundaries are computed per-date using `pytz Europe/Berlin`

---

## Temporal Model and Timezones

### Storage Rule

**All timestamps are stored as timezone-naive UTC.** This is the single most important rule for correct analytics.

```python
# What is stored (timezone-naive):
pd.Timestamp('2025-12-05 08:00:00')  # means 08:00 UTC

# What it represents for a Xetra trade:
# 08:00 UTC = 09:00 CET (winter) = first minute of continuous trading
```

### Converting UTC → CET/CEST in DuckDB

DuckDB does not automatically apply DST-aware timezone conversion. Use explicit UTC offsets per date:

```sql
-- Winter (CET = UTC+1): add 1 hour
SELECT trade_time + INTERVAL '1 hour' AS trade_time_cet, ...
FROM read_parquet('data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/trades.parquet');

-- Or use AT TIME ZONE (DuckDB 0.10+):
SELECT trade_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' AS trade_time_berlin, ...
```

### Converting UTC → CET/CEST in Python

```python
import pytz
import pandas as pd

berlin = pytz.timezone('Europe/Berlin')

df['trade_time_utc'] = df['trade_time']  # already UTC
df['trade_time_berlin'] = (
    df['trade_time']
    .dt.tz_localize('UTC')
    .dt.tz_convert('Europe/Berlin')
)
```

### Filtering to Trading Hours (Efficient Pattern)

Convert the **filter boundaries** to UTC once, not every timestamp in the file:

```python
from datetime import datetime, time, date
import pytz

def xetra_utc_bounds(trading_date: date):
    """Return (start_utc, end_utc) for Xetra continuous session, DST-aware."""
    berlin = pytz.timezone('Europe/Berlin')
    start = berlin.localize(datetime.combine(trading_date, time(9, 0)))
    end = berlin.localize(datetime.combine(trading_date, time(17, 30)))
    return start.astimezone(pytz.UTC), end.astimezone(pytz.UTC)

start_utc, end_utc = xetra_utc_bounds(date(2025, 12, 5))
df_session = df[(df['trade_time'] >= start_utc.replace(tzinfo=None)) &
               (df['trade_time'] <= end_utc.replace(tzinfo=None))]
```

### DST Transition Dates (relevant years)

| Year | CET → CEST | CEST → CET |
|------|------------|------------|
| 2025 | Mar 30     | Oct 26     |
| 2026 | Mar 29     | Oct 25     |

During CET (winter): `trade_time` 08:00 UTC = 09:00 local  
During CEST (summer): `trade_time` 07:00 UTC = 09:00 local

---

## Identifier Model

### Yahoo Finance: Ticker Symbols

Yahoo Finance data uses **ticker symbols** as the primary identifier (e.g. `AAPL`, `MSFT`, `BRK-B`). These:
- Are embedded in the partition path as `ticker=<SYMBOL>`
- Change on corporate actions (delistings, renames, mergers)
- Are US-market specific; the same company may have different symbols on different exchanges

### Xetra: ISINs

Xetra data uses **ISINs** (International Securities Identification Numbers) as the primary identifier (e.g. `DE0007100000` = Mercedes-Benz). These:
- Are 12-character strings: 2-letter country code + 9-digit NSIN + 1 check digit
- Are globally unique and exchange-independent
- Do not change on ticker renames (more stable than ticker symbols)
- Are embedded as `isin=<ISIN>` in `trades_by_isin/` and as `ticker=<ISIN>` in `stocks_*/` partitions

### Cross-Source Matching

Yahoo Finance and Xetra use different identifier systems. For cross-source analytics (e.g. compare German equities priced in EUR on Xetra vs. ADRs priced in USD on NYSE):
- A future ISIN→ticker mapping layer (Xetra Phase 3+) will provide the bridge
- For now, use a manual mapping table or an external reference (e.g. Deutsche Börse ISIN CSV) to join the two identifier spaces

---

## Data Quality and Caveats

### Yahoo Finance

| Caveat | Detail |
|--------|--------|
| 1m expiry | 1-minute data is permanently lost after 7 days if not collected |
| No adjustment | Prices are raw (not split/dividend adjusted); apply in analytics layer |
| Market hours only | Data covers regular trading hours; extended hours not collected |
| Gaps on holidays | No rows for market holidays or non-trading days |

### Xetra Raw Trades

| Caveat | Detail |
|--------|--------|
| 24h expiry | Raw tick data expires within 24 hours if not collected |
| 15-minute delay | Published data is delayed 15 minutes from execution time |
| MiFID II fields | Optional fields (`negotiated_flag` etc.) may be null — schema is padded with None for stability |
| Volume as float | `volume` is stored as `float64` (source format); cast to `int64` for share counts |
| Post-trade filings | Trades timestamped 18:00–22:00 CET are post-trade transparency reports, not live executions |
| Venue in row | The `venue` column inside the file is redundant with the `venue=` partition key |

### Xetra OHLCV (once implemented)

| Caveat | Detail |
|--------|--------|
| Aggregated from ticks | Not sourced directly from exchange feed; computed by `yf_parqed` |
| Session-filtered | Only 09:00–17:30 CET included; closing auction may shift last bar |
| ISIN as ticker | `ticker=` partition contains ISINs, not symbols, until mapping layer added |

---

## Data Completeness Monitoring

Both pipelines use a **capture rate** pattern to assess whether a given day's collection was complete: how many distinct minutes were captured vs. the theoretical session maximum.

### Yahoo Finance Completeness

```sql
-- Minutes captured per day vs. 390-minute US regular session
WITH minute_counts AS (
  SELECT
    CAST("date" AS DATE) AS trade_date,
    COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', "date")) AS minutes_with_trades
  FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet',
                    hive_partitioning=1)
  GROUP BY trade_date
)
SELECT
  trade_date,
  minutes_with_trades,
  390 AS theoretical_minutes,          -- 09:30–16:00 ET
  ROUND(100.0 * minutes_with_trades / 390, 2) AS capture_rate_pct,
  CASE
    WHEN minutes_with_trades >= 370 THEN 'Complete'
    WHEN minutes_with_trades >= 300 THEN 'Partial'
    ELSE 'Incomplete'
  END AS status
FROM minute_counts
ORDER BY trade_date DESC
LIMIT 10;
```

Thresholds: **Complete** ≥ 370 minutes (95%), **Partial** ≥ 300 minutes (77%).

### Xetra Completeness

```sql
-- Minutes captured per day vs. 600-minute active window (08:00–18:00 CET)
WITH daily_stats AS (
  SELECT
    CAST(trade_time AS DATE) AS trade_date,
    COUNT(*) AS trades_captured,
    COUNT(DISTINCT isin) AS unique_isins,
    COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', trade_time)) AS minutes_captured,
    ROUND(SUM(price * volume) / 1000000, 2) AS turnover_m_eur,
    MIN(trade_time) AS first_trade,
    MAX(trade_time) AS last_trade
  FROM read_parquet('/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet',
                    hive_partitioning=1)
  GROUP BY trade_date
)
SELECT
  trade_date,
  trades_captured,
  unique_isins,
  minutes_captured,
  600 AS theoretical_max_minutes,      -- 08:00–18:00 CET daemon window
  ROUND(100.0 * minutes_captured / 600, 2) AS capture_rate_pct,
  turnover_m_eur,
  strftime('%H:%M', first_trade) AS first_trade_utc,
  strftime('%H:%M', last_trade) AS last_trade_utc,
  CASE
    WHEN minutes_captured >= 540 THEN 'Complete'
    WHEN minutes_captured >= 450 THEN 'Partial'
    ELSE 'Incomplete'
  END AS status
FROM daily_stats
ORDER BY trade_date DESC
LIMIT 10;
```

Thresholds: **Complete** ≥ 540 minutes (90%), **Partial** ≥ 450 minutes (75%).

> Note: `first_trade_utc` and `last_trade_utc` in the Xetra query are UTC times. Add 1h (CET) or 2h (CEST) to convert to local market time.

---

## DuckDB Query Patterns

### Setup

```python
import duckdb

con = duckdb.connect()
DATA_ROOT = '/var/lib/yf_parqed/data'  # production path; substitute local path for dev
```

> **DuckDB gotcha**: The `date` column in Yahoo Finance parquet files must be quoted as `"date"` in SQL because `date` is a reserved keyword in DuckDB. Forgetting the quotes causes a parse error.

### Yahoo Finance

```sql
-- All 1d data, all tickers, all time
SELECT * FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=*/year=*/month=*/*.parquet',
    hive_partitioning=1
);

-- Single ticker, date range  ("date" must be quoted)
SELECT "date", open, high, low, close, volume
FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=AAPL/year=*/month=*/*.parquet',
    hive_partitioning=1
)
WHERE "date" BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY "date";

-- Ticker universe summary
SELECT ticker, COUNT(*) AS trading_days, MIN("date") AS first_date, MAX("date") AS last_date
FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=*/year=*/month=*/*.parquet',
    hive_partitioning=1
)
GROUP BY ticker
ORDER BY trading_days DESC;

-- Multi-ticker comparison (1d close prices)
SELECT "date", ticker, close
FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=*/year=2025/month=*/*.parquet',
    hive_partitioning=1
)
WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL')
ORDER BY ticker, "date";

-- Top 10 tickers by notional volume (use close * volume for Yahoo)
SELECT
    ticker,
    ROUND(SUM("close" * volume) / 1e6, 2) AS notional_volume_m_usd
FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=*/year=*/month=*/*.parquet',
    hive_partitioning=1
)
GROUP BY ticker
ORDER BY notional_volume_m_usd DESC
LIMIT 10;

-- Minute-level grouping (strftime for 1m interval)
SELECT
    strftime('%Y-%m-%d %H:%M', "date") AS minute,
    SUM(volume) AS total_volume
FROM read_parquet(
    '/var/lib/yf_parqed/data/us/yahoo/stocks_1m/ticker=AAPL/year=2025/month=12/*.parquet',
    hive_partitioning=1
)
GROUP BY minute
ORDER BY minute;
```

### Xetra Raw Trades (venue-partitioned)

> Use these patterns for month- or venue-level aggregations. For single-ISIN queries, wait for Phase 2a `trades_by_isin/` to avoid full-file scans. Notional volume for Xetra uses `price * volume` (not `close * volume` as with Yahoo).

```sql
-- Daily trade count and turnover by venue
SELECT venue, year, month, day,
       COUNT(*)                                AS trades,
       ROUND(SUM(price * volume) / 1e6, 2)    AS turnover_m_eur
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades/venue=*/year=*/month=*/day=*/*.parquet',
    hive_partitioning=1
)
GROUP BY venue, year, month, day
ORDER BY year DESC, month DESC, day DESC;

-- Top 20 ISINs by trade count for a specific month
SELECT isin, COUNT(*) AS trades, ROUND(SUM(price * volume) / 1e6, 2) AS turnover_m_eur
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=2025/month=12/day=*/*.parquet',
    hive_partitioning=1
)
GROUP BY isin
ORDER BY trades DESC
LIMIT 20;

-- Intraday trade distribution by hour (UTC — add 1h for CET, 2h for CEST)
SELECT EXTRACT(HOUR FROM trade_time) AS hour_utc,
       COUNT(*) AS trades
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/*.parquet',
    hive_partitioning=1
)
GROUP BY hour_utc
ORDER BY hour_utc;

-- Minute-level aggregation (strftime for tick data)
SELECT
    strftime('%Y-%m-%d %H:%M', trade_time) AS minute_utc,
    COUNT(*) AS trades,
    ROUND(SUM(price * volume) / 1e6, 4) AS turnover_m_eur
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/*.parquet',
    hive_partitioning=1
)
WHERE isin = 'DE0007100000'
GROUP BY minute_utc
ORDER BY minute_utc;
```

### Xetra Normalized Trades (Phase 2a, once available)

```sql
-- Single ISIN — efficient after Phase 2a (partition-pruned, ~115 KB)
SELECT trade_time, price, volume
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades_by_isin/venue=DETR/isin=DE0007100000/year=2025/month=12/*.parquet',
    hive_partitioning=1
)
ORDER BY trade_time;

-- VWAP for a single ISIN over a month
SELECT ROUND(SUM(price * volume) / SUM(volume), 4) AS vwap
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/trades_by_isin/venue=DETR/isin=DE0007100000/year=2025/month=12/*.parquet',
    hive_partitioning=1
);
```

### Xetra OHLCV (Phase 2b, once available)

```sql
-- Same pattern as Yahoo Finance — note "date" quoting still required
SELECT "date", open, high, low, close, volume
FROM read_parquet(
    '/var/lib/yf_parqed/data/de/xetra/stocks_1d/ticker=DE0007100000/year=*/month=*/*.parquet',
    hive_partitioning=1
)
ORDER BY "date";
```

---

## Cross-Source Analytics

Once Phase 2b is complete, the Yahoo Finance and Xetra OHLCV datasets share an identical schema (`date`, `open`, `high`, `low`, `close`, `volume`) and path structure (`stocks_<interval>/ticker=<ID>/year=<Y>/month=<M>/data.parquet`). This makes cross-source queries natural:

```sql
-- Compare daily closes from two sources (requires ISIN→ticker mapping)
-- Note: "date" must be quoted on both sides; prices are in different currencies
SELECT y."date", y.close AS aapl_usd, x.close AS mercedes_eur
FROM read_parquet('/var/lib/yf_parqed/data/us/yahoo/stocks_1d/ticker=AAPL/year=2025/month=*/*.parquet',
                  hive_partitioning=1) AS y
JOIN read_parquet('/var/lib/yf_parqed/data/de/xetra/stocks_1d/ticker=DE0007100000/year=2025/month=*/*.parquet',
                  hive_partitioning=1) AS x
  ON y."date" = x."date"
ORDER BY y."date";
```

**Caveats for cross-source comparison**:
- Prices are in different currencies (USD for Yahoo, EUR for Xetra)
- Trading hours overlap only partially (US market opens ~14:30 UTC, Xetra closes 15:30 UTC in summer)
- Yahoo prices are unadjusted; Xetra OHLCV is aggregated from ticks

---

## Storage Sizing Reference

| Dataset | Granularity | File size (typical) | Monthly total |
|---------|-------------|---------------------|---------------|
| Yahoo `stocks_1d` | 1 ticker/month | ~1 KB | ~5 MB (5k tickers) |
| Yahoo `stocks_1h` | 1 ticker/month | ~15 KB | ~75 MB (5k tickers) |
| Yahoo `stocks_1m` | 1 ticker/month | ~400 KB | ~2 GB (5k tickers) |
| Xetra `trades` (raw daily) | 1 venue/day | ~23 MB | ~690 MB (DETR) |
| Xetra `trades_by_isin` | 1 ISIN/month | ~115 KB avg (1 KB–20 MB) | ~480 MB (DETR) |
| Xetra `stocks_1m` | 1 ISIN/month | ~50 KB | ~210 MB (DETR) |
| Xetra `stocks_1h` | 1 ISIN/month | ~2 KB | ~8 MB (DETR) |
| Xetra `stocks_1d` | 1 ISIN/month | ~1 KB | ~4 MB (DETR) |

> ISIN distribution is highly skewed: top 1% of ISINs (~42 of ~4,200 on DETR) produce 5–20 MB files; bottom 50% produce < 10 KB files.

---

## Source Reference

| Document | Location |
|----------|----------|
| Storage structure (canonical) | `.github/STORAGE_STRUCTURE.md` |
| OHLCV aggregation design | `docs/adr/to-do/2025-12-05-ohlcv-aggregation-service.md` |
| Architecture overview | `.github/ARCHITECTURE.md` |
| Data safety rules | `.github/DATA_SAFETY_STRATEGY.md` |
| Release history | `docs/release-notes.md` |

**Last updated**: 2026-04-26
