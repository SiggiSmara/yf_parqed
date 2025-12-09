# ADR 2025-12-09: Normalized Analytics Layer

**Date**: 2025-12-09

## Status: Proposed (2025-12-09)

## Context

Raw OHLCV data (price in USD/EUR, volume in shares) is difficult to compare across:
- **Different securities**: AAPL at $180 vs GOOGL at $140 vs DBK at €14 - which is "moving more"?
- **Different markets**: US equities (USD) vs German equities (EUR)
- **Time periods**: 2020 vs 2025 (inflation, stock splits, market conditions)
- **Trading patterns**: Morning surge vs midday lull vs closing activity

Analysts need **comparable, dimensionless metrics** that reveal:
- Relative price movement (returns) independent of absolute price level
- Relative volume activity (volume z-score) independent of typical trading volume
- Intraday patterns (seasonality) that reveal liquidity and volatility cycles

**Dependency**: Builds on [ADR 2025-12-05: OHLCV Aggregation Service](../to-do/2025-12-05-ohlcv-aggregation-service.md) - assumes aggregated OHLCV data exists.

## Decision

Implement a **NormalizedAnalyticsService** that transforms raw OHLCV data into comparable, dimensionless representations:

1. **Price → Returns**: Log returns for easier comparison across securities and time
2. **Volume → Relative Volume**: Z-score vs adaptive moving averages + intraday seasonality adjustment
3. **Reusable service**: Works with both Yahoo Finance and Xetra data

### Design Decisions

#### 1. Price Normalization: Log Returns

**Decision**: Transform absolute prices to log returns for comparability.

**Formula**:
```python
log_return = ln(close_t / close_{t-1})

# Or for intraday (minute-level):
log_return = ln(close_minute / open_minute)
```

**Rationale**:
- **Dimensionless**: Returns are percentages, comparable across any price level
- **Additive**: Log returns can be summed across time periods
- **Symmetric**: +10% gain and -10% loss have equal magnitude in log space
- **Statistical properties**: Log returns are closer to normal distribution (better for analytics)

**Output Schema**:
```python
{
    "datetime": datetime,
    "close": float,           # Original close price (USD/EUR)
    "log_return": float,      # ln(close_t / close_{t-1})
    "return_pct": float,      # (close_t / close_{t-1} - 1) * 100
    "cumulative_return": float # Cumulative product of returns from period start
}
```

#### 2. Volume Normalization: Adaptive Relative Volume

**Decision**: Transform absolute volume to relative volume using adaptive moving averages and intraday seasonality.

**Two-Stage Normalization**:

**Stage 1: Detrend with Adaptive Moving Average**
```python
# Fast/slow EWMA using half-life specification (more intuitive than span)
# Half-life = days for weight to decay to 50%
# Relationship: span ≈ 1.44 * half_life (for continuous decay)

# Convert half-life to pandas span parameter
def halflife_to_span(halflife_days):
    """Convert half-life (days) to pandas EWMA span parameter."""
    return 2 * halflife_days - 1

# Fast EWMA: 5-day half-life (~10-day span) - responds to recent changes
volume_ewma_fast = volume.ewm(span=halflife_to_span(5)).mean()   # ~9 span

# Slow EWMA: 40-day half-life (~80-day span) - tracks long-term trend
volume_ewma_slow = volume.ewm(span=halflife_to_span(40)).mean()  # ~79 span

# Weighted average: 70% fast (recent), 30% slow (trend)
# Rationale: Prioritize recent activity for anomaly detection while maintaining trend awareness
volume_baseline = 0.7 * volume_ewma_fast + 0.3 * volume_ewma_slow

# Alternative: Lag-free moving average (e.g., ALMA, KAMA)
# volume_baseline = alma(volume, window=30, offset=0.85, sigma=6)

relative_volume_raw = volume / volume_baseline
```

**Stage 2: Seasonality Adjustment (Intraday)**
```python
# For minute-level data, adjust for trading hour patterns
# Morning surge (09:30-10:30), midday lull (11:30-14:30), closing surge (15:30-16:00)

hour_of_day = datetime.hour
minute_of_day = datetime.hour * 60 + datetime.minute

# Learn historical seasonality pattern (median volume by minute-of-day)
seasonality_factor = learn_seasonality(historical_volume, groupby='minute_of_day')

relative_volume_adjusted = relative_volume_raw / seasonality_factor[minute_of_day]

# Store baseline and adjusted volume for downstream model features
# (models can use raw relative_volume, adjusted, or both)
```

**Stage 3: Z-Score for Outlier Detection**
```python
# Separate z-score calculation for outlier/anomaly detection
# Use 20-day window (reasonable for detecting unusual activity vs recent baseline)
# Rolling window ensures z-score adapts to regime changes

rolling_window = 20  # trading days

volume_mean = relative_volume_adjusted.rolling(window=rolling_window, min_periods=10).mean()
volume_std = relative_volume_adjusted.rolling(window=rolling_window, min_periods=10).std()
volume_zscore = (relative_volume_adjusted - volume_mean) / volume_std

# Outlier thresholds:
# |z| > 2.0: Moderate outlier (95th percentile, ~5% of days)
# |z| > 3.0: Strong outlier (99.7th percentile, ~0.3% of days)
# |z| > 4.0: Extreme outlier (likely news event, earnings, etc.)
```

**Rationale**:
- **Half-life specification**: More intuitive than span (5-day half-life = "recent week matters most")
- **Fast/slow weighting**: 70/30 split favors recent activity (better for anomaly detection) while maintaining long-term context
- **Adaptive baseline**: EWMA responds to changing market conditions (bull/bear regime shifts)
- **Seasonality removal**: Isolates "unusual" volume from "expected" intraday patterns
- **Separate z-score stage**: 20-day window for outlier detection (independent from baseline calculation)
- **Dual use**: `relative_volume_adjusted` for model features, `volume_zscore` for outlier detection
- **Lag-free options**: ALMA/KAMA reduce lag vs simple moving averages (better for real-time use)

**Output Schema**:
```python
{
    "datetime": datetime,
    "volume": int,                      # Original volume (shares)
    "volume_ewma_fast": float,          # Fast EWMA (5-day half-life)
    "volume_ewma_slow": float,          # Slow EWMA (40-day half-life)
    "volume_baseline": float,           # Weighted average (70% fast + 30% slow)
    "relative_volume": float,           # volume / baseline (for models)
    "seasonality_factor": float,        # Expected volume ratio for this minute-of-day
    "relative_volume_adjusted": float,  # relative_volume / seasonality_factor (for models)
    "volume_zscore": float,             # Z-score vs 20-day window (for outlier detection)
    "volume_percentile": float          # Percentile rank (0-100) in rolling window
}
```

#### 3. Storage Structure

**Decision**: Store normalized data alongside raw OHLCV in separate datasets.

```
data/us/yahoo/
├── stocks_1m/              # Raw OHLCV (from aggregation ADR)
│   └── ticker=AAPL/...
├── normalized_1m/          # Normalized analytics
│   └── ticker=AAPL/
│       └── year=2025/month=12/
│           └── data.parquet
│
data/de/xetra/
├── stocks_1m/              # Raw OHLCV
│   └── venue=DETR/ticker=DE0005190003/...
├── normalized_1m/          # Normalized analytics
│   └── venue=DETR/ticker=DE0005190003/
│       └── year=2025/month=12/
│           └── data.parquet
```

**Rationale**:
- **Separation of concerns**: Raw OHLCV remains immutable, normalized data is derived
- **Independent updates**: Can recompute normalized data with different parameters without re-fetching
- **Storage efficiency**: Normalized data can be computed on-demand or materialized for performance
- **Schema flexibility**: Different normalization strategies don't affect raw data

#### 4. Moving Average Selection

**Decision**: Default to EWMA (fast/slow), provide pluggable interface for lag-free alternatives.

**EWMA (Default)**:
- **Pros**: Simple, well-understood, numpy/pandas native, interpretable half-life parameter
- **Cons**: Fixed decay rate (not adaptive to volatility)
- **Use case**: Most securities, baseline for comparison
- **Parameters**: Fast half-life = 5 days, Slow half-life = 40 days, Weight = 70/30

**ALMA (Arnaud Legoux Moving Average)**:
- **Pros**: Lag-free (offset parameter), configurable smoothness (sigma)
- **Cons**: Custom implementation needed, more parameters to tune
- **Use case**: Fast-moving securities, real-time applications

**KAMA (Kaufman Adaptive Moving Average)**:
- **Pros**: Adapts to volatility (faster in trends, slower in consolidation)
- **Cons**: Complex calculation, harder to tune
- **Use case**: Securities with regime changes (low vol → high vol)

**Implementation**:
```python
class VolumeNormalizer:
    def __init__(
        self, 
        ma_type='ewma', 
        fast_halflife=5,      # 5-day half-life for fast EWMA
        slow_halflife=40,     # 40-day half-life for slow EWMA
        fast_weight=0.7,      # 70% weight on fast EWMA
        zscore_window=20,     # 20-day window for z-score
        **kwargs
    ):
        self.ma_type = ma_type
        self.fast_halflife = fast_halflife
        self.slow_halflife = slow_halflife
        self.fast_weight = fast_weight
        self.zscore_window = zscore_window
        self.kwargs = kwargs
    
    def _halflife_to_span(self, halflife_days):
        """Convert half-life to pandas EWMA span parameter."""
        return 2 * halflife_days - 1
    
    def compute_baseline(self, volume_series):
        if self.ma_type == 'ewma':
            fast_span = self._halflife_to_span(self.fast_halflife)
            slow_span = self._halflife_to_span(self.slow_halflife)
            
            fast = volume_series.ewm(span=fast_span).mean()
            slow = volume_series.ewm(span=slow_span).mean()
            
            # Weighted average: default 70% fast, 30% slow
            return self.fast_weight * fast + (1 - self.fast_weight) * slow
        elif self.ma_type == 'alma':
            return self._alma(volume_series, **self.kwargs)
        elif self.ma_type == 'kama':
            return self._kama(volume_series, **self.kwargs)
        else:
            raise ValueError(f"Unknown MA type: {self.ma_type}")
    
    def compute_zscore(self, relative_volume_adjusted):
        """Compute z-score for outlier detection using separate rolling window."""
        mean = relative_volume_adjusted.rolling(
            window=self.zscore_window, 
            min_periods=int(self.zscore_window / 2)
        ).mean()
        std = relative_volume_adjusted.rolling(
            window=self.zscore_window,
            min_periods=int(self.zscore_window / 2)
        ).std()
        return (relative_volume_adjusted - mean) / std
```

#### 5. Seasonality Learning

**Decision**: Learn historical seasonality patterns from data, store in reference table.

**Learning Process**:
```python
# For each ticker, aggregate historical volume by minute-of-day
# Use median (robust to outliers) over last 3 months

seasonality_profile = historical_volume.groupby('minute_of_day').median()

# Normalize so mean factor = 1.0 (preserves total volume)
seasonality_profile = seasonality_profile / seasonality_profile.mean()

# Store in reference table
seasonality_table = pd.DataFrame({
    'minute_of_day': range(0, 390),  # 09:30-16:00 ET = 390 minutes
    'seasonality_factor': seasonality_profile.values
})
```

**Storage**:
```
data/reference/
├── seasonality_us_equity.parquet    # US market intraday pattern
├── seasonality_xetra.parquet        # Xetra market intraday pattern
└── seasonality_per_ticker/          # Ticker-specific overrides
    ├── AAPL.parquet
    └── DE0005190003.parquet
```

**Rationale**:
- **Market-level defaults**: Most securities follow similar intraday patterns
- **Ticker-specific overrides**: High-frequency traders, ETFs may deviate
- **Recompute periodically**: Weekly/monthly updates capture evolving patterns

## Implementation Architecture

### NormalizedAnalyticsService

```python
class NormalizedAnalyticsService:
    """
    Transform raw OHLCV data to normalized, comparable representations.
    
    Design Principles:
    - Source-agnostic: Works with Yahoo Finance and Xetra data
    - Pluggable MAs: Support EWMA, ALMA, KAMA via interface
    - Seasonality-aware: Adjusts for intraday trading patterns
    - Stateless: No internal state, pure transformation
    """
    
    def __init__(
        self,
        price_normalizer: PriceNormalizer,
        volume_normalizer: VolumeNormalizer,
        seasonality_provider: SeasonalityProvider
    ):
        self.price_normalizer = price_normalizer
        self.volume_normalizer = volume_normalizer
        self.seasonality_provider = seasonality_provider
    
    def normalize(
        self,
        ohlcv_df: pd.DataFrame,
        ticker: str,
        market: str = "us"
    ) -> pd.DataFrame:
        """
        Normalize OHLCV data to returns and relative volume.
        
        Args:
            ohlcv_df: Raw OHLCV data (output from OHLCVAggregator)
            ticker: Security identifier for seasonality lookup
            market: Market identifier (us, de) for default seasonality
        
        Returns:
            DataFrame with normalized columns added
        """
        # 1. Price normalization (returns)
        normalized = ohlcv_df.copy()
        normalized['log_return'] = self.price_normalizer.compute_log_returns(
            normalized['close']
        )
        normalized['return_pct'] = (np.exp(normalized['log_return']) - 1) * 100
        
        # 2. Volume normalization (baseline with fast/slow EWMA)
        baseline_result = self.volume_normalizer.compute_baseline_detailed(
            normalized['volume']
        )
        normalized['volume_ewma_fast'] = baseline_result['fast']
        normalized['volume_ewma_slow'] = baseline_result['slow']
        normalized['volume_baseline'] = baseline_result['baseline']
        normalized['relative_volume'] = (
            normalized['volume'] / normalized['volume_baseline']
        )
        
        # 3. Seasonality adjustment
        seasonality = self.seasonality_provider.get_seasonality(
            ticker=ticker,
            market=market
        )
        normalized['seasonality_factor'] = normalized.index.to_series().apply(
            lambda dt: seasonality.get(dt.hour * 60 + dt.minute, 1.0)
        )
        normalized['relative_volume_adjusted'] = (
            normalized['relative_volume'] / normalized['seasonality_factor']
        )
        
        # 4. Z-score for outlier detection (separate 20-day window)
        normalized['volume_zscore'] = self.volume_normalizer.compute_zscore(
            normalized['relative_volume_adjusted']
        )
        
        return normalized
```

### CLI Integration

```bash
# Normalize existing OHLCV data
yf-parqed normalize --ticker AAPL --interval 1m --date 2025-12-09
xetra-parqed normalize --venue DETR --ticker DE0005190003 --interval 1m --date 2025-12-09

# Compute seasonality profiles
yf-parqed learn-seasonality --market us --lookback-days 90
xetra-parqed learn-seasonality --venue DETR --lookback-days 90

# Query normalized data (DuckDB integration)
yf-parqed query normalized --ticker AAPL --interval 1m --start 2025-12-01 --end 2025-12-09
```

## Use Cases

### 1. Cross-Security Comparison

**Problem**: Which stock is "moving more" today?

**Solution**: Compare log returns (dimensionless, comparable)
```sql
SELECT 
    ticker,
    SUM(log_return) as total_return,
    STDDEV(log_return) as volatility,
    MAX(volume_zscore) as max_volume_spike
FROM read_parquet('data/*/normalized_1d/ticker=*/year=2025/month=12/*.parquet')
WHERE date = '2025-12-09'
GROUP BY ticker
ORDER BY ABS(total_return) DESC;
```

### 2. Cross-Market Comparison

**Problem**: Are US or German equities more volatile this week?

**Solution**: Compare volatility (stddev of log returns)
```sql
SELECT 
    market,
    AVG(STDDEV(log_return)) as avg_volatility,
    AVG(AVG(volume_zscore)) as avg_relative_volume
FROM read_parquet('data/{us,de}/*/normalized_1d/ticker=*/year=2025/month=12/*.parquet')
WHERE date >= '2025-12-02' AND date <= '2025-12-09'
GROUP BY market;
```

### 3. Intraday Pattern Analysis

**Problem**: When is liquidity highest for AAPL?

**Solution**: Analyze volume z-score by minute-of-day
```sql
SELECT 
    EXTRACT(hour FROM datetime) as hour,
    EXTRACT(minute FROM datetime) as minute,
    AVG(volume_zscore) as avg_zscore,
    AVG(relative_volume_adjusted) as avg_relative_volume
FROM read_parquet('data/us/yahoo/normalized_1m/ticker=AAPL/year=2025/month=12/*.parquet')
GROUP BY hour, minute
ORDER BY hour, minute;
```

### 4. Anomaly Detection

**Problem**: Detect unusual trading activity (volume spikes)

**Solution**: Alert when volume_zscore > 3 (3 standard deviations above mean)
```sql
SELECT 
    ticker,
    datetime,
    volume,
    volume_zscore,
    relative_volume_adjusted
FROM read_parquet('data/*/normalized_1m/ticker=*/year=2025/month=12/*.parquet')
WHERE volume_zscore > 3.0
ORDER BY volume_zscore DESC;
```

## Testing Strategy

### Unit Tests

```python
# tests/test_normalized_analytics_service.py
def test_log_returns_computation():
    """Test log returns match manual calculation."""
    prices = pd.Series([100, 105, 103, 108])
    normalizer = PriceNormalizer()
    
    log_returns = normalizer.compute_log_returns(prices)
    
    # ln(105/100) = 0.04879
    assert np.isclose(log_returns.iloc[1], 0.04879, atol=1e-4)

def test_volume_baseline_ewma():
    """Test EWMA baseline calculation with half-life parameters."""
    volume = pd.Series([1000, 1100, 1050, 1200, 1150])
    normalizer = VolumeNormalizer(
        ma_type='ewma', 
        fast_halflife=2,   # 2-day half-life
        slow_halflife=5,   # 5-day half-life
        fast_weight=0.7    # 70% fast, 30% slow
    )
    
    baseline = normalizer.compute_baseline(volume)
    
    assert len(baseline) == len(volume)
    assert baseline.iloc[-1] > volume.iloc[0]  # Baseline adapts to recent data
    
    # Fast EWMA should be closer to recent values than slow
    fast_span = 2 * 2 - 1  # 3
    slow_span = 2 * 5 - 1  # 9
    fast_ewma = volume.ewm(span=fast_span).mean()
    slow_ewma = volume.ewm(span=slow_span).mean()
    expected_baseline = 0.7 * fast_ewma + 0.3 * slow_ewma
    
    assert np.allclose(baseline, expected_baseline, rtol=1e-5)

def test_zscore_window_independence():
    """Test that z-score uses independent 20-day window."""
    # Create 50 days of volume data with spike on day 40
    volume = pd.Series([1000] * 39 + [5000] + [1000] * 10)
    normalizer = VolumeNormalizer(
        ma_type='ewma',
        fast_halflife=5,
        slow_halflife=40,
        zscore_window=20  # Separate window for z-score
    )
    
    baseline = normalizer.compute_baseline(volume)
    relative_volume = volume / baseline
    zscore = normalizer.compute_zscore(relative_volume)
    
    # Spike should show high z-score (using 20-day window around it)
    spike_zscore = zscore.iloc[39]
    assert spike_zscore > 3.0  # Strong outlier
    
    # Days before spike should have normal z-scores
    assert zscore.iloc[20:35].abs().max() < 2.0

def test_seasonality_normalization():
    """Test seasonality adjustment removes intraday patterns."""
    # Create synthetic data with known seasonality
    volume = create_volume_with_seasonality(
        base=1000,
        morning_surge=1.5,  # 50% higher in morning
        midday_lull=0.7     # 30% lower at midday
    )
    
    provider = SeasonalityProvider.from_data(volume)
    adjusted = volume / provider.get_seasonality_series(volume.index)
    
    # Adjusted volume should have uniform mean across time
    morning_mean = adjusted.between_time('09:30', '10:30').mean()
    midday_mean = adjusted.between_time('12:00', '14:00').mean()
    assert np.isclose(morning_mean, midday_mean, rtol=0.1)

def test_cross_security_comparability():
    """Test normalized returns are comparable across price levels."""
    # Two stocks: AAPL at $180, DBK at €14 (both gain 5%)
    aapl_prices = pd.Series([180, 189])
    dbk_prices = pd.Series([14, 14.7])
    
    normalizer = PriceNormalizer()
    aapl_returns = normalizer.compute_log_returns(aapl_prices)
    dbk_returns = normalizer.compute_log_returns(dbk_prices)
    
    # Both should have ~same log return (ln(1.05) = 0.04879)
    assert np.isclose(aapl_returns.iloc[1], dbk_returns.iloc[1], atol=1e-4)
```

### Integration Tests

```python
# tests/test_normalized_end_to_end.py
def test_normalize_ohlcv_data(tmp_path):
    """Test full normalization pipeline on real OHLCV data."""
    # Setup: Create sample OHLCV data
    ohlcv = create_sample_ohlcv(ticker='AAPL', days=30, interval='1m')
    
    # Execute: Normalize
    service = NormalizedAnalyticsService(
        price_normalizer=PriceNormalizer(),
        volume_normalizer=VolumeNormalizer(ma_type='ewma'),
        seasonality_provider=SeasonalityProvider(market='us')
    )
    normalized = service.normalize(ohlcv, ticker='AAPL', market='us')
    
    # Verify: Schema and data quality
    assert 'log_return' in normalized.columns
    assert 'volume_zscore' in normalized.columns
    assert normalized['log_return'].notna().all()
    assert -1 < normalized['log_return'].mean() < 1  # Reasonable returns
    assert -5 < normalized['volume_zscore'].mean() < 5  # Z-score centered
```

## Alternatives Considered

### Simple Returns vs Log Returns

**Simple Returns**: `(close_t / close_{t-1} - 1) * 100`
- **Pros**: Intuitive (10% gain/loss)
- **Cons**: Not additive, asymmetric (-50% then +50% ≠ 0%)

**Log Returns**: `ln(close_t / close_{t-1})`
- **Pros**: Additive, symmetric, normal distribution
- **Cons**: Less intuitive (0.04879 = ~5%)

**Decision**: Use log returns for analytics, provide `return_pct` for human-readable output.

### Volume Normalization Methods

**Simple MA**: `volume / sma(volume, 20)`
- **Pros**: Simple, interpretable
- **Cons**: Lags recent changes

**EWMA**: `volume / ewma(volume, span=20)`
- **Pros**: Recent data weighted more
- **Cons**: Still lags, uniform decay

**Z-score + Seasonality**: `(volume / baseline - mean) / std`, adjusted for intraday patterns
- **Pros**: Dimensionless, removes seasonality, comparable
- **Cons**: More complex, requires historical data

**Decision**: Use Z-score + seasonality for maximum comparability, provide `relative_volume` for simpler use cases.

## Work Log

| Date       | Milestone | Status | Notes |
|------------|-----------|--------|-------|
| 2025-12-09 | Draft ADR | Proposed | Initial proposal; depends on OHLCV aggregation ADR |
|            | PriceNormalizer implementation | Pending | Log returns, cumulative returns |
|            | VolumeNormalizer implementation | Pending | EWMA baseline, z-score |
|            | SeasonalityProvider implementation | Pending | Learn/store intraday patterns |
|            | NormalizedAnalyticsService | Pending | Orchestration layer |
|            | CLI commands | Pending | normalize, learn-seasonality |
|            | Unit tests | Pending | Returns, baseline, seasonality |
|            | Integration tests | Pending | End-to-end normalization |
|            | DuckDB query examples | Pending | Cross-security/market comparison |

## Consequences

### Benefits

- **Comparability**: Returns and z-scores are dimensionless, work across any security/market
- **Flexibility**: Pluggable MA types (EWMA, ALMA, KAMA) support different use cases
- **Seasonality-aware**: Removes predictable intraday patterns, isolates anomalies
- **Foundation for ML**: Normalized features are better inputs for machine learning models
- **⚡ Eliminates Split Adjustment Dependency**: Log returns work correctly on unadjusted prices, making corporate action tracking **optional instead of required** for most analytics

**Corporate Actions Impact**:

This normalization approach fundamentally changes how we handle stock splits:

```python
# Traditional problem: Need split metadata to adjust historical prices
# Example: AAPL 4-for-1 split on 2020-08-31
# Without adjustment: [400, 100, 101, 102] ← discontinuity breaks analysis
# With adjustment:    [100, 100, 101, 102] ← requires split tracking database

# Normalized solution: Returns automatically handle splits
log_returns = ln([400, 100, 101, 102] / [NaN, 400, 100, 101])
            = [NaN, -1.386, 0.00995, 0.00985]

# Split day shows extreme outlier (-138.6% "return")
# Detection: if |log_return| > 0.5 (>50%), flag as corporate action
# Treatment: Exclude that day or interpolate from surrounding returns

# Result: Volatility, correlation, risk metrics work without split metadata!
```

**Corporate Action Detection (Built-in)**:

The normalized analytics layer includes automatic detection:

```python
def detect_corporate_actions(log_returns: pd.Series, threshold: float = 0.26) -> pd.Index:
    """
    Detect likely corporate actions via return outliers.
    
    Thresholds:
    - 0.26 (30% overnight) → Likely split/reverse split
    - 0.51 (67% overnight) → Definite split (2-for-1 minimum)
    - 0.69 (100% overnight) → Large split or merger
    """
    outliers = log_returns[log_returns.abs() > threshold]
    return outliers.index

def clean_returns(log_returns: pd.Series, action_days: pd.Index) -> pd.Series:
    """
    Remove or interpolate returns on corporate action days.
    """
    cleaned = log_returns.copy()
    cleaned.loc[action_days] = np.nan  # Mark as missing
    cleaned = cleaned.interpolate(method='linear')  # Or just drop
    return cleaned
```

**Impact on OHLCV Aggregation ADR**:

- **Simplifies Phase 2**: OHLCV aggregation can store **unadjusted prices** (simpler, no external dependencies)
- **Defers Phase 3**: Corporate action tracking becomes **optional enhancement** rather than required infrastructure
- **Use Cases**:
  - ✓ **Works without adjustment**: Volatility, correlation, returns analysis, risk metrics, ML features
  - ✗ **Still needs adjustment**: Price charts, absolute price strategies, backtesting (if using price-based rules)

**Recommendation**: Implement normalized analytics layer in Phase 2, defer corporate action metadata to Phase 3+ (only if users need adjusted price charts)

### Costs

- **Storage overhead**: Normalized data doubles storage requirements (can compute on-demand)
- **Compute cost**: Seasonality learning requires historical data aggregation
- **Complexity**: More parameters to tune (MA spans, seasonality lookback)
- **Dependency**: Requires OHLCV aggregation ADR implementation first

### Risks

- **Parameter sensitivity**: MA half-lives, weights, and z-score window affect results, need careful tuning
- **Regime changes**: Historical seasonality may not apply during market structure changes
- **Edge cases**: Low-volume securities may have unstable z-scores
- **Fast/slow weighting**: 70/30 split is heuristic, optimal ratio may vary by security type

### Mitigations

- **Sensible defaults**: Use empirically validated parameters:
  - Fast EWMA: 5-day half-life (responds to recent activity)
  - Slow EWMA: 40-day half-life (tracks long-term trend)
  - Weight: 70% fast / 30% slow (favors anomaly detection)
  - Z-score window: 20 days (balances responsiveness vs stability)
  - Seasonality lookback: 90 days (3 months of intraday patterns)
- **Validation**: Alert when z-scores exceed reasonable bounds (|z| > 5)
- **Fallbacks**: If seasonality data unavailable, use market-level defaults
- **Documentation**: Provide parameter tuning guide with worked examples
- **Dual output**: Provide both `relative_volume_adjusted` (for models) and `volume_zscore` (for outliers) so users can choose appropriate metric

## Dependencies

- **Required**: [ADR 2025-12-05: OHLCV Aggregation Service](../to-do/2025-12-05-ohlcv-aggregation-service.md) - Normalized analytics operates on aggregated OHLCV data
- **Optional**: [ADR 2025-10-12: DuckDB Query Layer](2025-10-12-duckdb-query-layer.md) - Enhanced by normalized data for cross-security queries

## References

- **Log Returns**: [Wikipedia - Rate of Return](https://en.wikipedia.org/wiki/Rate_of_return#Logarithmic_or_continuously_compounded_return)
- **EWMA**: [Pandas EWMA Documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.ewm.html)
- **ALMA**: [TradingView ALMA Indicator](https://www.tradingview.com/support/solutions/43000594683-arnaud-legoux-moving-average/)
- **Intraday Seasonality**: Admati, A. R., & Pfleiderer, P. (1988). "A Theory of Intraday Patterns: Volume and Price Variability." The Review of Financial Studies.
