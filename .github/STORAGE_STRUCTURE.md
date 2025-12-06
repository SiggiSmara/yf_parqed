# Storage Structure Reference

Comprehensive guide to yf_parqed's data storage organization, partition schemes, and file layouts.

## Overview

yf_parqed uses **Hive-style partitioning** for efficient data organization and querying. All data lives under the `data/` directory with market/source/dataset hierarchy.

## Directory Structure

```
data/
├── us/                          # United States market
│   └── yahoo/                   # Yahoo Finance source
│       ├── stocks_1m/           # 1-minute OHLCV data
│       ├── stocks_1h/           # 1-hour OHLCV data
│       └── stocks_1d/           # 1-day OHLCV data
│
├── de/                          # German market
│   └── xetra/                   # Deutsche Börse Xetra source
│       ├── trades/              # Raw per-trade data (daily partitions)
│       ├── trades_monthly/      # Consolidated monthly trade data
│       ├── stocks_1m/           # 1-minute OHLCV (aggregated, Phase 2)
│       ├── stocks_1h/           # 1-hour OHLCV (aggregated, Phase 2)
│       └── stocks_1d/           # 1-day OHLCV (aggregated, Phase 2)
│
└── legacy/                      # Pre-migration flat structure (deprecated)
    ├── stocks_1m/
    ├── stocks_1h/
    └── stocks_1d/
```

---

## Yahoo Finance Storage

### Path Pattern

```
data/us/yahoo/stocks_<interval>/ticker=<TICKER>/year=<YYYY>/month=<MM>/data.parquet
```

### Examples

```
data/us/yahoo/stocks_1d/ticker=AAPL/year=2025/month=12/data.parquet
data/us/yahoo/stocks_1d/ticker=MSFT/year=2025/month=11/data.parquet
data/us/yahoo/stocks_1h/ticker=GOOGL/year=2025/month=12/data.parquet
data/us/yahoo/stocks_1m/ticker=TSLA/year=2025/month=12/data.parquet
```

### Partition Keys

| Level | Key | Description | Example |
|-------|-----|-------------|---------|
| 1 | Market | Two-letter country code | `us` |
| 2 | Source | Data provider name | `yahoo` |
| 3 | Dataset | `stocks_<interval>` | `stocks_1d` |
| 4 | Ticker | Stock symbol | `ticker=AAPL` |
| 5 | Year | Four-digit year | `year=2025` |
| 6 | Month | Two-digit month (zero-padded) | `month=12` |

### Parquet Schema

```python
{
    "date": datetime64[ns],      # Trading date (index)
    "open": float64,             # Opening price
    "high": float64,             # High price
    "low": float64,              # Low price
    "close": float64,            # Closing price
    "volume": int64,             # Trading volume
    "dividends": float64,        # Dividend amount (if any)
    "stock_splits": float64      # Split ratio (if any)
}
```

### File Naming

- **Single file per partition**: `data.parquet`
- **Monthly partitions**: One file contains all trading days for that month
- **Updates**: Existing file is read, merged with new data, deduplicated, and rewritten

---

## Xetra Storage

### Raw Trades (Current - Phase 1)

#### Path Pattern

```
data/de/xetra/trades/venue=<VENUE>/year=<YYYY>/month=<MM>/day=<DD>/trades.parquet
```

#### Examples

```
data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/trades.parquet
data/de/xetra/trades/venue=DFRA/year=2025/month=11/day=28/trades.parquet
data/de/xetra/trades/venue=DGAT/year=2025/month=12/day=03/trades.parquet
```

#### Partition Keys

| Level | Key | Description | Example |
|-------|-----|-------------|---------|
| 1 | Market | Two-letter country code | `de` |
| 2 | Source | Data provider name | `xetra` |
| 3 | Dataset | `trades` for raw data | `trades` |
| 4 | Venue | Trading venue code | `venue=DETR` |
| 5 | Year | Four-digit year | `year=2025` |
| 6 | Month | Two-digit month (zero-padded) | `month=12` |
| 7 | Day | Two-digit day (zero-padded) | `day=05` |

#### Parquet Schema

```python
{
    "message_id": string,           # Message identifier
    "source_name": string,          # Source system name
    "isin": string,                 # International Securities Identification Number
    "instrument_id": string,        # Instrument identifier
    "trans_id": string,             # Transaction ID
    "tick_id": string,              # Tick ID
    "price": float64,               # Trade price (EUR)
    "volume": int64,                # Trade volume (shares)
    "currency": string,             # Currency code (EUR)
    "quote_type": string,           # Quote type
    "trade_time": datetime64[ns],   # Trade timestamp
    "distribution_time": datetime64[ns],  # Data distribution timestamp
    "venue": string,                # Trading venue code
    "tick_action": string,          # Tick action
    "instrument_code": string,      # Instrument code
    "market_mechanism": string,     # Market mechanism
    "trading_mode": string,         # Trading mode
    "negotiated_flag": string,      # Negotiated trade flag
    "modification_flag": string,    # Modification flag
    "benchmark_flag": string,       # Benchmark flag
    "pub_deferral": string,         # Publication deferral flag
    "algo_indicator": bool          # Algorithmic indicator
}
```

#### File Naming

- **Daily files**: `trades.parquet`
- **One file per venue per day**: Contains all trades for that venue on that date
- **Trading hours filtering**: Data limited to market hours (08:00-18:00 CET by default)

### Monthly Consolidation

#### Path Pattern

```
data/de/xetra/trades_monthly/venue=<VENUE>/year=<YYYY>/month=<MM>/trades.parquet
```

#### Examples

```
data/de/xetra/trades_monthly/venue=DETR/year=2025/month=12/trades.parquet
data/de/xetra/trades_monthly/venue=DFRA/year=2025/month=11/trades.parquet
```

#### Purpose

- **Space optimization**: Consolidates daily files into monthly archives
- **Query performance**: Fewer files to scan for month-level queries
- **Retention**: Daily files can be deleted after consolidation

---

## Xetra OHLCV (Future - Phase 2)

### Path Pattern

```
data/de/xetra/stocks_<interval>/ticker=<ISIN_OR_TICKER>/year=<YYYY>/month=<MM>/data.parquet
```

### Examples (Planned)

```
# By ISIN (unmapped tickers)
data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1h/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1d/ticker=DE0005190003/year=2025/month=12/data.parquet

# By ticker symbol (mapped tickers, Phase 2.5)
data/de/xetra/stocks_1d/ticker=BMW/year=2025/month=12/data.parquet
```

### Partition Keys

| Level | Key | Description | Example |
|-------|-----|-------------|---------|
| 1 | Market | Two-letter country code | `de` |
| 2 | Source | Data provider name | `xetra` |
| 3 | Dataset | `stocks_<interval>` | `stocks_1d` |
| 4 | Ticker/ISIN | Stock identifier | `ticker=DE0005190003` |
| 5 | Year | Four-digit year | `year=2025` |
| 6 | Month | Two-digit month (zero-padded) | `month=12` |

### Parquet Schema (Planned)

```python
{
    "date": datetime64[ns],         # Trading date (index)
    "open": float64,                # Opening price (EUR)
    "high": float64,                # High price (EUR)
    "low": float64,                 # Low price (EUR)
    "close": float64,               # Closing price (EUR)
    "volume": int64,                # Trading volume
    "isin": string,                 # ISIN for reference
    "source_interval": string,      # Source data interval ("tick", "1m")
    "aggregated_at": datetime64[ns] # Aggregation timestamp
}
```

---

## Querying Data

### DuckDB Examples

#### Yahoo Finance Data

```sql
-- All tickers for a specific interval
SELECT * FROM read_parquet(
    'data/us/yahoo/stocks_1d/ticker=*/year=*/month=*/*.parquet',
    hive_partitioning=1
);

-- Specific ticker
SELECT * FROM read_parquet(
    'data/us/yahoo/stocks_1d/ticker=AAPL/year=*/month=*/*.parquet',
    hive_partitioning=1
);

-- Ticker count and date range
SELECT 
    COUNT(DISTINCT ticker) as total_tickers,
    MIN("date") as first_date,
    MAX("date") as last_date
FROM read_parquet(
    'data/us/yahoo/stocks_1d/ticker=*/year=*/month=*/*.parquet',
    hive_partitioning=1
);
```

#### Xetra Raw Trades

```sql
-- All venues for a specific month
SELECT * FROM read_parquet(
    'data/de/xetra/trades/venue=*/year=2025/month=12/day=*/*.parquet',
    hive_partitioning=1
);

-- Specific venue and date range
SELECT * FROM read_parquet(
    'data/de/xetra/trades/venue=DETR/year=2025/month=12/day=*/*.parquet',
    hive_partitioning=1
);

-- Daily trade summary by partition day
SELECT 
    day,
    COUNT(*) as trades,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur
FROM read_parquet(
    'data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet',
    hive_partitioning=1
)
GROUP BY day
ORDER BY day DESC;

-- Daily trade summary by trade_time
SELECT 
    CAST(trade_time AS DATE) as trade_date,
    COUNT(*) as trades,
    ROUND(SUM(price * volume) / 1000000, 2) as volume_millions_eur
FROM read_parquet(
    'data/de/xetra/trades/venue=DETR/year=*/month=*/day=*/*.parquet',
    hive_partitioning=1
)
GROUP BY trade_date
ORDER BY trade_date DESC;
```

### Pandas Examples

#### Yahoo Finance Data

```python
import pandas as pd

# Read specific ticker
df = pd.read_parquet('data/us/yahoo/stocks_1d/ticker=AAPL/year=2025/month=12/data.parquet')

# Read all tickers for a date range (using glob)
from glob import glob
files = glob('data/us/yahoo/stocks_1d/ticker=*/year=2025/month=12/*.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])
```

#### Xetra Raw Trades

```python
import pandas as pd

# Read specific venue and day
df = pd.read_parquet('data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/trades.parquet')

# Read all days for a month
from glob import glob
files = glob('data/de/xetra/trades/venue=DETR/year=2025/month=12/day=*/trades.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])
```

---

## Legacy Storage (Deprecated)

### Path Pattern

```
data/legacy/stocks_<interval>/<TICKER>.parquet
```

### Examples

```
data/legacy/stocks_1d/AAPL.parquet
data/legacy/stocks_1h/MSFT.parquet
data/legacy/stocks_1m/GOOGL.parquet
```

### Migration

**Command:**
```bash
# Move legacy data
mv stocks_1d data/legacy/
mv stocks_1h data/legacy/
mv stocks_1m data/legacy/

# Migrate to partitioned storage
uv run yf-parqed-migrate migrate --venue us:yahoo --interval 1d
uv run yf-parqed-migrate migrate --venue us:yahoo --interval 1h
uv run yf-parqed-migrate migrate --venue us:yahoo --interval 1m
```

**See:** [DATA_SAFETY_STRATEGY.md](DATA_SAFETY_STRATEGY.md) for migration guidelines.

---

## Storage Sizing

### Typical Sizes

| Data Type | Interval | Rows/Day/Ticker | Size/Month/Ticker | Compression |
|-----------|----------|-----------------|-------------------|-------------|
| Yahoo Finance | 1d | 1 | ~1 KB | Gzip |
| Yahoo Finance | 1h | 6-7 | ~15 KB | Gzip |
| Yahoo Finance | 1m | 390 | ~400 KB | Gzip |
| Xetra Trades | tick | ~50,000 | ~50 MB | Snappy |
| Xetra OHLCV | 1m | ~390 | ~50 KB | Gzip |
| Xetra OHLCV | 1h | ~10 | ~2 KB | Gzip |
| Xetra OHLCV | 1d | 1 | ~1 KB | Gzip |

### Scaling Examples

**5,000 Yahoo Finance tickers:**
- 1d: ~5 MB/month
- 1h: ~75 MB/month
- 1m: ~2 GB/month

**Xetra (4 venues, 500 ISINs):**
- Raw trades: ~200 MB/day, ~6 GB/month
- OHLCV 1m: ~100 MB/month
- OHLCV 1h/1d: ~5 MB/month

---

## Implementation References

### Code Files

- **Path construction**: `src/yf_parqed/partition_path_builder.py`
- **Yahoo Finance storage**: `src/yf_parqed/partitioned_storage_backend.py`
- **Xetra storage**: `src/yf_parqed/xetra_service.py`
- **Migration logic**: `src/yf_parqed/partition_migration_service.py`

### Documentation

- **Architecture**: `ARCHITECTURE.md` - Service responsibilities
- **ADRs**:
  - `docs/adr/2025-10-12-partition-aware-storage.md` - Partition storage design
  - `docs/adr/2025-10-12-xetra-delayed-data.md` - Xetra Phase 1 implementation
  - `docs/adr/2025-12-05-ohlcv-aggregation-service.md` - Xetra Phase 2 OHLCV aggregation

---

**Last Updated:** 2025-12-05  
**Version:** 0.4.0
