# Rate Limit Discovery Test Suite

**Created**: 2025-11-03  
**Purpose**: Empirically determine Deutsche Börse API rate limits  
**Status**: Ready for manual execution

---

## What We Built

A comprehensive test suite to systematically probe the Deutsche Börse API and discover:

1. **Burst capacity** - How many rapid requests before 429?
2. **Sustained rate** - What req/s can we maintain long-term?
3. **Optimal configuration** - What's the fastest safe setting?
4. **Recovery time** - How long to wait after hitting 429?

---

## Quick Usage

```bash
# Run all discovery tests (5-10 minutes, ~100 API requests)
uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s

# Run just one test
uv run pytest tests/test_xetra_rate_limit_discovery.py::TestXetraRateLimitDiscovery::test_sustained_load_5_per_2s -v -s
```

---

## Test Suite Components

### 6 Discovery Tests

1. **`test_baseline_sequential_downloads`**
   - 10 downloads with no rate limiting
   - Measures natural API behavior
   - ~30 seconds

2. **`test_rapid_fire_burst`**
   - Up to 20 rapid downloads (stops on 429)
   - Finds instant burst limit
   - ~5 seconds

3. **`test_sustained_load_5_per_2s`**
   - 20 downloads at default rate (2.5 req/s)
   - Validates current configuration
   - ~8 seconds

4. **`test_sustained_load_10_per_2s`**
   - 20 downloads at aggressive rate (5 req/s)
   - Tests if we can go faster
   - ~4 seconds

5. **`test_find_maximum_sustainable_rate`**
   - Tests 5 different configurations
   - Binary search for optimal rate
   - ~5 minutes (includes 30s waits)

6. **`test_recovery_time_after_429`**
   - Deliberately triggers 429
   - Tests recovery at 10s/20s/30s/45s/60s intervals
   - ~3 minutes

### Total: ~10 minutes, ~100 API requests

---

## Example Output

### Successful Configuration
```
=== Sustained Load Test (5 req/2s) ===
Total downloads: 20
Successful: 20
Rate limited (429): 0
Total time: 8.23s
Effective rate: 2.43 req/s

✓ SUCCESS: No rate limits hit at 5 req/2s (2.5 req/s)
  This configuration is SAFE for production use
```

### Failed Configuration
```
=== Sustained Load Test (10 req/2s) ===
Total downloads: 20
Successful: 15
Rate limited (429): 5
Total time: 4.87s

⚠️  Rate limited 5 times
  First 429 at request #16
  10 req/2s (5 req/s) is TOO AGGRESSIVE
```

### Summary Report
```
============================================================
RATE LIMIT DISCOVERY SUMMARY
============================================================
Config          Rate   Success    Limited     Status
------------------------------------------------------------
3 req/2s         1.5/s        15          0    ✓ SAFE
5 req/2s         2.5/s        15          0    ✓ SAFE
7 req/2s         3.5/s        12          3    ✗ FAILS
10 req/2s        5.0/s         8          7    ✗ FAILS
15 req/2s        7.5/s         5         10    ✗ FAILS

✓ RECOMMENDED CONFIGURATION:
  5 req/2s (2.5 req/s)
```

---

## How to Use Results

### Step 1: Run Discovery Tests

```bash
uv run pytest tests/test_xetra_rate_limit_discovery.py::TestXetraRateLimitDiscovery::test_find_maximum_sustainable_rate -v -s
```

### Step 2: Identify Safe Configuration

Look for the highest rate with **0 rate limit errors**:
- ✓ SAFE = Use this or lower
- ✗ FAILS = Too aggressive

### Step 3: Apply Safety Margin

If test shows 7 req/2s is safe, use 6 req/2s in production (20% margin).

### Step 4: Update Configuration

Edit `src/yf_parqed/config_service.py`:

```python
def __init__(self, base_path: Path | None = None):
    # ... existing code ...
    
    # Based on discovery test results:
    self._xetra_max_requests = 6  # Found 7 works, using 6 for safety
    self._xetra_duration = 2
```

### Step 5: Validate

Run integration tests to ensure new config works:

```bash
uv run pytest tests/test_xetra_integration.py -v
```

---

## Safety Notes

⚠️ **IMPORTANT**:
- Tests make ~100 real API requests
- Will deliberately trigger 429 errors
- May temporarily block your IP
- Takes 5-10 minutes to complete

✅ **Best Practices**:
- Run during off-peak hours
- Run from dev machine, not production
- Wait 1 hour between test runs
- Only run when validating rate limits
- DO NOT run in CI/CD

---

## Test Markers

Tests use:
- `@pytest.mark.live` - Requires real API
- `@pytest.mark.slow` - Takes several minutes

**Skipped by default**:
```bash
uv run pytest  # Skips these tests
```

**Run explicitly**:
```bash
uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s
```

---

## Why This Matters

### Before Discovery Tests
- Guessing at safe rate limits
- Conservative settings (1.5 req/s)
- 1800 files = 20 minutes
- Don't know if we could go faster

### After Discovery Tests
- Know actual API limits empirically
- Can use optimal settings (e.g., 2.5 req/s)
- 1800 files = 12 minutes (40% faster)
- Confidence in configuration

### Production Impact

For daily downloads of all 4 venues:
- Before: 4 venues × 1800 files × 0.67s = **80 minutes**
- After: 4 venues × 1800 files × 0.40s = **48 minutes**
- **Saved: 32 minutes per day** (40% improvement)

---

## Files Created

1. **`tests/test_xetra_rate_limit_discovery.py`** (485 lines)
   - 6 discovery tests
   - Systematic rate limit probing
   - Detailed result reporting

2. **`docs/xetra/RATE_LIMIT_DISCOVERY.md`** (327 lines)
   - Usage guide
   - Test descriptions
   - Interpretation guide
   - Troubleshooting

3. **`docs/xetra/RATE_LIMIT_DISCOVERY_SUMMARY.md`** (this file)
   - Quick overview
   - Impact summary

---

## Next Steps

1. **Run discovery tests** (manually, one time):
   ```bash
   uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s
   ```

2. **Record results** in implementation checklist

3. **Update configuration** based on findings

4. **Re-run periodically** (monthly/quarterly) to detect API changes

5. **Monitor production** for 429 errors in logs

---

## Integration with Existing Work

### Complements Rate Limiting Implementation

- **Already built**: Proactive rate limiter (`enforce_limits()`)
- **Already built**: Configurable via `ConfigService`
- **Already built**: Exponential backoff retry
- **NEW**: Empirical data to set optimal defaults

### Enhances Phase 1.5 Testing

- **Mocked tests**: Validate logic (23 tests)
- **Live API tests**: Validate real behavior (7 tests)
- **Discovery tests**: Optimize configuration (6 tests)

---

## Documentation References

- Full usage guide: `docs/xetra/RATE_LIMIT_DISCOVERY.md`
- Implementation details: `docs/xetra/RATE_LIMITING_IMPLEMENTATION.md`
- Test suite: `tests/test_xetra_rate_limit_discovery.py`
- Configuration: `src/yf_parqed/config_service.py`

---

**Total Test Count**: 284 tests
- 265 baseline (non-Xetra)
- 13 rate limiting unit tests
- 6 discovery tests (manual only)

**All 278 automated tests passing** ✓
