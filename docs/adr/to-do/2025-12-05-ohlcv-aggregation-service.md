# ADR: OHLCV Aggregation Service

## Status: To-Do (agreed; work not started)

**Context**: Xetra Phase 2 and 3 Implementation

## Problem Statement

We store raw tick-level data from two sources:
1. **Yahoo Finance**: Pre-aggregated OHLCV at 1m/1h/1d intervals
2. **Xetra**: Raw per-trade data (price, volume, timestamp per transaction)

Xetra raw trades need aggregation to OHLCV format for:
- Consistency with Yahoo Finance data model
- Efficient time-series analysis (daily/hourly patterns)
- Reduced data volume for common queries
- Enable cross-source analytics (compare US vs German equities)

**Key Challenge**: Yahoo Finance API returns inconsistent data across intervals (different adjustment logic, varying columns). We need a unified aggregation approach that works for both sources.

## Decision

Implement a **single source of truth** architecture where:
1. Fetch and store **only raw data** from APIs (1m for Yahoo, raw trades for Xetra)
2. Aggregate locally using pandas to produce derived intervals (1h, 1d from raw data)
3. Create reusable `OHLCVAggregator` service that works for both data sources

### Design Decisions

#### 1. Storage Structure

**Decision**: Use `stocks_<interval>` naming to match Yahoo Finance convention.

```
data/de/xetra/stocks_1m/venue=DETR/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1h/venue=DETR/ticker=DE0005190003/year=2025/month=12/data.parquet
data/de/xetra/stocks_1d/venue=DETR/ticker=DE0005190003/year=2025/month=12/data.parquet
```

**Rationale**:
- Consistency with existing Yahoo Finance storage (`data/us/yahoo/stocks_1d/...`)
- Simplifies cross-source queries (same path pattern)
- Clear separation: `trades/` for raw data, `stocks_<interval>/` for OHLCV
- Future DuckDB analytics can query both sources uniformly

**Alternative Considered**: `ohlcv_<interval>` naming to make aggregation explicit. Rejected for consistency with Yahoo naming.

#### 2. Aggregation Trigger

**Decision**: Explicit CLI command for aggregation (manual control).

```bash
# User explicitly triggers aggregation
xetra-parqed aggregate-ohlcv DETR --interval 1m --date 2025-12-05
xetra-parqed aggregate-ohlcv DETR --interval 1h --date 2025-12-05
xetra-parqed aggregate-ohlcv DETR --interval 1d --date 2025-12-05
```

**Rationale**:
- **Maximum control**: User decides when to aggregate (e.g., after verifying raw data quality)
- **Debugging friendly**: Separate command makes it easy to test aggregation logic in isolation
- **Migration path**: Transition to eager/automatic aggregation once workflow is established
- **Resource management**: Aggregation is CPU-intensive, users control when it runs

**Future Evolution**:
- Phase 2.5: Add `--auto-aggregate` flag to `fetch-trades` command (opt-in eager)
- Phase 3: Enable by default in daemon mode after proving stability

**Alternatives Considered**:
- **On-demand (lazy)**: Aggregate during reads. Rejected: unpredictable latency, cache invalidation complexity
- **Eager (automatic)**: Aggregate after every fetch. Rejected: couples fetching and aggregation, harder to debug

#### 3. Ticker Identification

**Decision**: Use ISIN for storage keys, defer ticker symbol lookup.

```
# Phase 2: Store by ISIN (as found in raw trades)
data/de/xetra/stocks_1d/ticker=DE0005190003/...

# Phase 2.5: Add ISIN→Ticker lookup service
# Phase 3: Support both ISIN and ticker symbol queries
```

**Rationale**:
- **Data integrity**: ISIN is authoritative identifier in Xetra raw trades
- **No external dependencies**: Ticker lookup requires additional API/database
- **Simplicity**: One identifier, one storage location, no synchronization issues
- **Partition key stability**: ISIN never changes, ticker symbols can (rare but possible)

**Future Enhancement**: Add `ISINRegistry` service with ISIN→Ticker mapping for user-friendly queries.

#### 4. Corporate Actions

**Decision**: Naive aggregation without adjustments (Phase 2), add adjustment capability later.

**Rationale**:
- **Phase 2 Goal**: Prove aggregation pipeline works end-to-end
- **Data availability**: Xetra raw trades don't include split/dividend metadata
- **External dependency**: Requires additional data source for corporate actions
- **Complexity**: Adjustment logic is intricate, needs separate ADR and implementation phase

**Phase 3 Roadmap**:
1. Implement `CorporateActionService` to fetch split/dividend data
2. Add `AdjustmentEngine` to apply adjustments to OHLCV data
3. Store both adjusted and unadjusted OHLCV (like Yahoo Finance)
4. Document adjustment methodology in separate ADR

## Implementation Architecture

### OHLCVAggregator Service

```python
class OHLCVAggregator:
    """
    Reusable aggregation service for converting tick/minute data to OHLCV bars.
    
    Design Principles:
    - Source-agnostic: Works with any DataFrame containing price/volume/timestamp
    - Schema-flexible: Adapts to different column names via normalization
    - Time-zone aware: Handles market timezone conversions
    - Stateless: No internal state, pure transformation
    """
    
    def __init__(self, normalizer: DataNormalizer, tz_converter: TimezoneConverter):
        self.normalizer = normalizer
        self.tz_converter = tz_converter
    
    def aggregate(
        self,
        df: pd.DataFrame,
        source_interval: str,  # "tick", "1m"
        target_interval: str,  # "1h", "1d"
        market_tz: str = "Europe/Berlin"
    ) -> pd.DataFrame:
        """
        Aggregate raw data to OHLCV bars.
        
        Args:
            df: Raw data with price/volume/timestamp columns
            source_interval: Granularity of input ("tick" for trades, "1m" for minute bars)
            target_interval: Desired output interval ("1h", "1d")
            market_tz: Market timezone for aggregation boundaries
        
        Returns:
            DataFrame with OHLCV columns and datetime index
        
        Raises:
            ValueError: If df is empty or missing required columns
        """
        # 1. Normalize schema (handle different column names)
        normalized = self.normalizer.normalize(df)
        
        # 2. Convert to market timezone for correct aggregation boundaries
        normalized.index = self.tz_converter.to_market_tz(normalized.index, market_tz)
        
        # 3. Resample using pandas built-in OHLCV aggregation
        # Aggregate price and volume and also capture trade counts and VWAP
        grouped = normalized.resample(target_interval).agg({
            'price': ['first', 'max', 'min', 'last', 'count'],  # Open, High, Low, Close, trade_count
            'volume': 'sum'
        })

        # Flatten multi-index columns and name them explicitly
        grouped.columns = ['open', 'high', 'low', 'close', 'trade_count', 'volume']

        # Compute VWAP: sum(price * volume) / sum(volume) per group
        # (pandas resample with custom aggregator shown conceptually)
        pv = normalized.assign(price_x_volume=(normalized['price'] * normalized['volume']))
        vwap = pv.resample(target_interval)['price_x_volume'].sum() / pv.resample(target_interval)['volume'].sum()
        grouped['vwap'] = vwap

        # Remove bars with no trades (volume=0)
        ohlcv = grouped[grouped['volume'] > 0].copy()

        # Add provenance and metadata columns
        ohlcv['source_interval'] = source_interval
        ohlcv['aggregated_at'] = datetime.now()
        ohlcv['aggregated_by'] = 'OHLCVAggregator'
        ohlcv['aggregation_version'] = '1.0'
        # Optionally store source file references or checksums for audit
        ohlcv['source_files'] = None

        return ohlcv
```

### DataNormalizer

```python
class DataNormalizer:
    """
    Normalize heterogeneous data sources to common schema.
    
    Handles:
    - Different column names (Price vs price vs Close)
    - Different timestamp formats (Unix, ISO8601, datetime)
    - Different data types (int64 vs float64)
    """
    
    def normalize(self, df: pd.DataFrame, source: str = "xetra") -> pd.DataFrame:
        """
        Normalize to standard schema: index=datetime, columns=[price, volume]
        """
        if source == "xetra":
            return self._normalize_xetra_trades(df)
        elif source == "yahoo":
            return self._normalize_yahoo_minute(df)
        else:
            raise ValueError(f"Unknown source: {source}")
    
    def _normalize_xetra_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Xetra raw trades schema:
        - Time (Unix timestamp)
        - StartPrice (first trade price)
        - EndPrice (last trade price)
        - TradedVolume (total volume)
        """
        normalized = pd.DataFrame({
            'price': df['EndPrice'],  # Use last trade price
            'volume': df['TradedVolume']
        })
        normalized.index = pd.to_datetime(df['Time'], unit='ms')
        return normalized
    
    def _normalize_yahoo_minute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Yahoo Finance 1m data already in OHLCV format.
        Extract close price for tick-level aggregation.
        """
        normalized = pd.DataFrame({
            'price': df['close'],
            'volume': df['volume']
        })
        normalized.index = df.index
        return normalized
```

### CLI Integration

New command in `xetra_cli.py`:

```python
@app.command()
def aggregate_ohlcv(
    venue: str = typer.Argument(..., help="Trading venue (e.g., DETR)"),
    interval: str = typer.Option(..., help="Target interval: 1m, 1h, 1d"),
    date: Optional[str] = typer.Option(None, help="Date to aggregate (YYYY-MM-DD)"),
    source_interval: str = typer.Option("tick", help="Source data interval"),
    market: str = typer.Option("de", help="Market identifier"),
    source: str = typer.Option("xetra", help="Data source name"),
):
    """
    Aggregate raw trade data to OHLCV bars.
    
    Examples:
        # Aggregate today's trades to 1-minute bars
        xetra-parqed aggregate-ohlcv DETR --interval 1m
        
        # Aggregate specific date to hourly bars
        xetra-parqed aggregate-ohlcv DETR --interval 1h --date 2025-12-05
        
        # Aggregate to daily bars
        xetra-parqed aggregate-ohlcv DETR --interval 1d --date 2025-12-05
    """
    service = XetraService()
    
    # Determine date range
    target_date = parse_date(date) if date else datetime.now().date()
    
    # Load raw trades for date
    trades_df = service.load_trades(venue, target_date, market, source)
    
    if trades_df.empty:
        typer.echo(f"No raw trades found for {venue} on {target_date}")
        raise typer.Exit(1)
    
    # Aggregate to target interval
    aggregator = OHLCVAggregator(normalizer=DataNormalizer(), tz_converter=TimezoneConverter())
    ohlcv_df = aggregator.aggregate(
        trades_df,
        source_interval=source_interval,
        target_interval=interval,
        market_tz="Europe/Berlin"
    )
    
    # Save to partitioned storage
    service.save_ohlcv(ohlcv_df, venue, interval, target_date, market, source)
    
    typer.echo(f"✓ Aggregated {len(ohlcv_df)} {interval} bars for {venue} on {target_date}")
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1 (Current): Raw Data Collection                         │
└─────────────────────────────────────────────────────────────────┘

## Correctness & Validation

To ensure aggregation correctness and detect regressions, the aggregator must implement the following validation and fallback behaviors:

- **Deduplication / ordering**: Before aggregation, normalize and deduplicate input trades.
    - Primary dedupe key: transaction id (if present). Fallback: `(timestamp, price, volume, isin)` tuple.
    - Sort by timestamp (then by transaction id) to determine open and close prices deterministically for colliding timestamps.

- **Volume conservation checks**: After aggregation, assert that the sum of `volume` across output bars equals the sum of input trade volumes for the same range. If mismatch > threshold (configurable, e.g., 0.01%), treat as failure.

- **Minute-coverage and completeness**: Report minutes covered vs expected trading minutes per session and fail or warn when coverage drops below configured thresholds. Emit metrics: `aggregation.minutes_covered`, `aggregation.capture_rate_pct`.

- **Provenance & checksums**: Record `source_files` or their checksums used to produce each partition and store them alongside the aggregated partition for audit and potential rollback.

- **Validation failure behavior**:
    - On non-critical validation warnings (e.g., minor coverage shortfall), write the aggregated partition to a staging location and mark `status=partial` in the manifest; alert operators.
    - On critical validation failures (volume mismatch, schema issues), do not promote staging file to active partition; keep staging file for investigation and emit an alert with details. Optionally store a failure artifact for diagnostics.

- **Idempotence & resumability**: Aggregations write to a staging path and produce a per-partition manifest file with progress checkpoints. Re-running aggregation in resume mode should continue from the last successful checkpoint without reprocessing already-verified partitions.

- **Atomic activation**: After successful validation, perform an atomic replace of staging → active partition (same-directory temp + fsync + replace). Update manifest to record `verified_at` and `checksum`.

- **Testing & edge-cases**: Add unit/integration tests that cover:
    - Duplicated trades and out-of-order arrivals
    - Late-arriving trades (ensure resume and merge semantics)
    - DST changes and half-day sessions
    - Empty intervals, single-trade minutes, and multiple trades per millisecond

These validations are expected to be executed by the aggregator service and its test harness prior to activating any aggregated partition.
    │
    │ xetra-parqed fetch-trades DETR
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ data/de/xetra/trades/venue=DETR/year=2025/month=12/day=05/     │
│   └── trades.parquet  (raw per-trade data)                      │
└─────────────────────────────────────────────────────────────────┘
    │
    │ xetra-parqed aggregate-ohlcv DETR --interval 1m
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ OHLCVAggregator Service                                         │
│   1. Load raw trades from parquet                               │
│   2. Normalize schema (Time→datetime, EndPrice→price)           │
│   3. Resample to target interval (1m/1h/1d)                     │
│   4. Generate OHLCV columns (open, high, low, close, volume)   │
│   5. Filter zero-volume bars                                    │
└─────────────────────────────────────────────────────────────────┘
    │
    │ PartitionedStorageBackend.save()
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (1-minute OHLCV bars)                       │
│                                                                  │
│ data/de/xetra/stocks_1h/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (hourly OHLCV bars)                         │
│                                                                  │
│ data/de/xetra/stocks_1d/ticker=DE0005190003/year=2025/month=12/ │
│   └── data.parquet  (daily OHLCV bars)                          │
└─────────────────────────────────────────────────────────────────┘
```

## Testing Strategy

### Unit Tests

```python
# tests/test_ohlcv_aggregator.py
def test_aggregate_trades_to_1m_bars():
    """Test aggregation from tick data to 1-minute bars."""
    trades = create_mock_trades(num=100, timespan="1h")
    aggregator = OHLCVAggregator(...)
    
    result = aggregator.aggregate(trades, source_interval="tick", target_interval="1m")
    
    assert len(result) <= 60  # Max 60 1-minute bars in 1 hour
    assert result['volume'].sum() == trades['TradedVolume'].sum()  # Volume conservation
    assert result['high'].max() >= result['low'].min()  # OHLC consistency

def test_aggregate_1m_to_1h_bars():
    """Test aggregation from 1-minute to hourly bars."""
    minute_bars = create_mock_ohlcv(interval="1m", count=60)
    aggregator = OHLCVAggregator(...)
    
    result = aggregator.aggregate(minute_bars, source_interval="1m", target_interval="1h")
    
    assert len(result) == 1  # One hourly bar from 60 minute bars
    assert result['open'].iloc[0] == minute_bars['open'].iloc[0]  # First open
    assert result['close'].iloc[0] == minute_bars['close'].iloc[-1]  # Last close

def test_timezone_aware_aggregation():
    """Test that aggregation respects market timezone boundaries."""
    # Create trades spanning midnight UTC
    trades = create_trades_around_midnight(tz="UTC")
    aggregator = OHLCVAggregator(...)
    
    # Aggregate to daily bars in Berlin timezone
    result = aggregator.aggregate(trades, source_interval="tick", target_interval="1d", market_tz="Europe/Berlin")
    
    # Should create bars aligned to Berlin midnight, not UTC midnight
    assert result.index[0].tz == ZoneInfo("Europe/Berlin")
```

### Integration Tests

```python
# tests/test_xetra_aggregation_integration.py
def test_full_aggregation_pipeline(tmp_path):
    """Test complete workflow: fetch → store → aggregate → verify."""
    service = XetraService(working_path=tmp_path)
    
    # 1. Fetch raw trades (mocked API)
    with mock_xetra_api():
        service.fetch_trades("DETR", date(2025, 12, 5))
    
    # 2. Aggregate to 1-minute bars
    service.aggregate_ohlcv("DETR", interval="1m", date=date(2025, 12, 5))
    
    # 3. Verify output exists and is valid
    ohlcv_path = tmp_path / "data/de/xetra/stocks_1m/ticker=DE0005190003/year=2025/month=12/data.parquet"
    assert ohlcv_path.exists()
    
    df = pd.read_parquet(ohlcv_path)
    assert 'open' in df.columns
    assert 'high' in df.columns
    assert 'low' in df.columns
    assert 'close' in df.columns
    assert 'volume' in df.columns
    assert len(df) > 0

def test_aggregation_preserves_data_integrity():
    """Verify that aggregation doesn't lose or corrupt data."""
    trades = load_real_trades("DETR", date(2025, 12, 5))
    total_volume = trades['TradedVolume'].sum()
    
    # Aggregate to 1m, 1h, 1d
    ohlcv_1m = aggregator.aggregate(trades, "tick", "1m")
    ohlcv_1h = aggregator.aggregate(trades, "tick", "1h")
    ohlcv_1d = aggregator.aggregate(trades, "tick", "1d")
    
    # Volume must be conserved across all intervals
    assert ohlcv_1m['volume'].sum() == total_volume
    assert ohlcv_1h['volume'].sum() == total_volume
    assert ohlcv_1d['volume'].sum() == total_volume
```

### End-to-End Tests

```python
# tests/test_xetra_cli_aggregation.py
def test_aggregate_ohlcv_cli_command():
    """Test CLI command execution."""
    runner = CliRunner()
    
    # Setup test data
    setup_mock_trades("DETR", date(2025, 12, 5))
    
    # Run CLI command
    result = runner.invoke(app, [
        "aggregate-ohlcv", "DETR",
        "--interval", "1m",
        "--date", "2025-12-05"
    ])
    
    assert result.exit_code == 0
    assert "Aggregated" in result.stdout
    assert "1m bars" in result.stdout
```

## Migration Path to Eager Aggregation

Once workflow is proven stable:

```python
# Phase 2.5: Add opt-in auto-aggregation
@app.command()
def fetch_trades(
    venue: str,
    date: Optional[str] = None,
    auto_aggregate: bool = typer.Option(False, help="Automatically aggregate to OHLCV after fetch")
):
    """Fetch raw trades with optional auto-aggregation."""
    service = XetraService()
    
    # Fetch raw trades
    service.fetch_trades(venue, parse_date(date))
    
    # Optionally aggregate
    if auto_aggregate:
        for interval in ["1m", "1h", "1d"]:
            service.aggregate_ohlcv(venue, interval, parse_date(date))
            typer.echo(f"✓ Auto-aggregated to {interval}")

# Phase 3: Enable by default in daemon mode
@app.command()
def daemon(
    venue: str,
    auto_aggregate: bool = typer.Option(True, help="Auto-aggregate after fetches (default: enabled)")
):
    """Run daemon with auto-aggregation."""
    # Default to eager aggregation in production
    ...
```

## Performance Considerations

### Pandas Resample Efficiency

- **Memory**: Resample operates on in-memory DataFrame, requires ~2x source data size
- **CPU**: Single-threaded, scales linearly with row count
- **I/O**: Read once (raw trades), write once (OHLCV)

**Benchmarks** (estimated for typical Xetra day):
- Raw trades: ~500K rows, ~50 MB
- Aggregate to 1m: ~2 seconds, output ~400 rows, ~50 KB
- Aggregate to 1h: ~1 second, output ~10 rows, ~2 KB
- Aggregate to 1d: <1 second, output ~1 row, <1 KB

### Storage Overhead

- Raw trades: 50 MB/day/venue
- OHLCV 1m: 50 KB/day/venue (~0.1% of raw)
- OHLCV 1h: 2 KB/day/venue (~0.004% of raw)
- OHLCV 1d: <1 KB/day/venue (~0.002% of raw)

**Total**: ~50 MB/day (dominated by raw trades)

### Scalability

Current architecture supports:
- **Tickers**: Unlimited (per-ticker partitioning)
- **Time range**: Years of history (monthly partitions)
- **Concurrent aggregation**: Parallelize by date or ticker if needed (future enhancement)

## Alternatives Considered

### 1. DuckDB for Aggregation

**Pros**: SQL interface, potential performance gains for large datasets
**Cons**: Additional dependency, more complex for simple resampling, overkill for Phase 2

**Decision**: Defer to Phase 3 for analytics use cases, not ETL.

### 2. Store Only Raw Data, Aggregate On-Demand

**Pros**: Single source of truth, no storage redundancy
**Cons**: Unpredictable latency, repeated computation, cache invalidation complexity

**Decision**: Pre-compute OHLCV, disk is cheap and queries are fast.

### 3. Fetch Pre-Aggregated OHLCV from Xetra API

**Pros**: No aggregation logic needed
**Cons**: Xetra API doesn't provide OHLCV, only raw trades

**Decision**: Not possible, must aggregate locally.

## Success Metrics

Phase 2 implementation is successful when:

1. ✅ **Correctness**: Aggregated OHLCV matches manual verification (spot-check 10 tickers)
2. ✅ **Performance**: Aggregation completes in <5 seconds per day per venue
3. ✅ **Storage**: OHLCV files are <1% size of raw trades
4. ✅ **Usability**: CLI command is intuitive, documentation is clear
5. ✅ **Testability**: 100% pass rate on aggregation test suite (unit + integration)
6. ✅ **Maintainability**: OHLCVAggregator service is reusable for Yahoo Finance data

## Future Enhancements (Phase 3+)

1. **Corporate Action Adjustments**: Implement split/dividend adjustments
2. **ISIN→Ticker Lookup**: Add user-friendly ticker symbol queries
3. **Automatic Aggregation**: Enable by default in daemon mode
4. **DuckDB Analytics**: Zero-copy SQL queries on OHLCV data
5. **Cross-Source Comparison**: Unified queries across Yahoo + Xetra
6. **Real-Time Aggregation**: Streaming aggregation for live trading data

## References

- [Xetra Phase 1 ADR](2025-10-12-xetra-delayed-data.md)
- [Partition Storage ADR](2025-10-12-partition-aware-storage.md)
- [DuckDB Query Layer ADR](2025-10-12-duckdb-query-layer.md)
- [Yahoo Finance Pipeline ADR](2025-10-10-yahoo-finance-data-pipeline.md)

---

**Approved By**: SiggiSmara  
**Implementation Start**: 2025-12-05
