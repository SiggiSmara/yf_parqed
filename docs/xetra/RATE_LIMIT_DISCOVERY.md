# Xetra Rate Limit Discovery Tests

**Purpose**: Systematically probe the Deutsche Börse API to discover actual rate limits and optimize configuration.

**Status**: Experimental - Run manually, not in CI

---

## Quick Start

### Run All Discovery Tests

```bash
# Run all rate limit discovery tests
uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s

# This will take 5-10 minutes and make ~100 API requests
```

### Run Individual Tests

```bash
# Just test default configuration (5 req/2s)
uv run pytest tests/test_xetra_rate_limit_discovery.py::TestXetraRateLimitDiscovery::test_sustained_load_5_per_2s -v -s

# Find maximum sustainable rate
uv run pytest tests/test_xetra_rate_limit_discovery.py::TestXetraRateLimitDiscovery::test_find_maximum_sustainable_rate -v -s

# Quick burst test to find instant rate limit
uv run pytest tests/test_xetra_rate_limit_discovery.py::TestXetraRateLimitDiscovery::test_rapid_fire_burst -v -s
```

---

## Test Descriptions

### 1. `test_baseline_sequential_downloads`

**Purpose**: Measure unthrottled download performance  
**Requests**: 10 downloads with no rate limiting  
**Duration**: ~30 seconds  
**Goal**: Establish baseline and see if we hit 429 naturally

**Example Output**:
```
=== Baseline Sequential Downloads ===
Total downloads: 10
Successful: 7
Rate limited (429): 3
Total time: 4.52s
Average per download: 0.45s
Effective rate: 2.21 req/s

⚠️  Hit rate limit after 7 successful downloads
   Suggests limit is around 7 requests in 4.52s
```

### 2. `test_rapid_fire_burst`

**Purpose**: Find instant burst capacity before 429  
**Requests**: Up to 20 rapid downloads (stops on first 429)  
**Duration**: ~5 seconds  
**Goal**: Discover burst window size

**Example Output**:
```
=== Rapid Fire Burst Test ===
Total requests attempted: 12
Successful before rate limit: 11
Total time: 3.21s

⚠️  RATE LIMIT HIT!
   First 429 error at request #12
   After 11 successful requests in 3.21s
   Burst capacity: ~11 requests
   Burst rate: 3.43 req/s
```

### 3. `test_sustained_load_5_per_2s`

**Purpose**: Validate default configuration (5 req/2s)  
**Requests**: 20 downloads at 2.5 req/s  
**Duration**: ~8 seconds  
**Goal**: Confirm default is safe for production

**Example Output**:
```
=== Sustained Load Test (5 req/2s) ===
Total downloads: 20
Successful: 20
Rate limited (429): 0
Total time: 8.23s (expected minimum: 8.00s)
Effective rate: 2.43 req/s

✓ SUCCESS: No rate limits hit at 5 req/2s (2.5 req/s)
  This configuration is SAFE for production use
```

### 4. `test_sustained_load_10_per_2s`

**Purpose**: Test aggressive configuration (10 req/2s)  
**Requests**: 20 downloads at 5 req/s  
**Duration**: ~4 seconds  
**Goal**: See if we can safely use higher limits

**Example Output**:
```
=== Sustained Load Test (10 req/2s) ===
Total downloads: 20
Successful: 15
Rate limited (429): 5
Total time: 4.87s
Effective rate: 4.11 req/s

⚠️  Rate limited 5 times
  First 429 at request #16
  10 req/2s (5 req/s) is TOO AGGRESSIVE
```

### 5. `test_find_maximum_sustainable_rate`

**Purpose**: Binary search for optimal rate  
**Requests**: 15 downloads × 5 configurations = 75 total  
**Duration**: ~5 minutes (includes 30s waits between tests)  
**Goal**: Find highest safe rate limit

**Example Output**:
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
  This completed 15 downloads without rate limiting
```

### 6. `test_recovery_time_after_429`

**Purpose**: Measure how long to wait after hitting 429  
**Requests**: 20-30 downloads with timed recovery attempts  
**Duration**: ~3 minutes  
**Goal**: Optimize retry delay after rate limiting

**Example Output**:
```
=== Phase 1: Triggering Rate Limit ===
Request 1: SUCCESS
Request 2: SUCCESS
...
Request 11: ✓ HIT 429 (as expected)

=== Phase 2: Testing Recovery After 10s Wait ===
Waiting 10 seconds...
✗ Still rate limited after 10s

=== Phase 2: Testing Recovery After 20s Wait ===
Waiting 20 seconds...
✗ Still rate limited after 20s

=== Phase 2: Testing Recovery After 30s Wait ===
Waiting 30 seconds...
✓ SUCCESS after 30s wait - rate limit recovered
  Downloaded 46,240 bytes

RECOMMENDATION: Wait at least 30s after 429 errors
```

---

## Expected Results

Based on initial testing (Phase 1.5), we expect:

| Configuration | Expected Result |
|---------------|-----------------|
| 3 req/2s (1.5/s) | ✓ Always safe |
| 5 req/2s (2.5/s) | ✓ Default - should be safe |
| 7 req/2s (3.5/s) | ? Unknown |
| 10 req/2s (5/s) | ⚠️ Likely to hit 429 |
| Burst: 10-15 req | ⚠️ Likely limit |
| Recovery time | ~30-60 seconds |

---

## Interpreting Results

### Safe Configuration

If a test shows **0 rate limit errors (429)**:
- Configuration is SAFE for production
- Can use this rate for batch downloads
- Example: "5 req/2s" completing 20 downloads with 0 errors

### Aggressive Configuration

If a test shows **>0 rate limit errors**:
- Configuration is TOO AGGRESSIVE
- Will cause retries and delays in production
- Use a more conservative setting

### Recommended Action

1. Run `test_find_maximum_sustainable_rate` first
2. Note which configuration had:
   - 0 rate limit errors
   - Highest req/s rate
3. Use that configuration as your production default
4. Add 20% safety margin (e.g., if 7 req/2s works, use 6 req/2s)

---

## Updating Production Configuration

Based on test results, update `config_service.py`:

```python
# Before discovery tests (conservative guess)
def __init__(self, base_path: Path | None = None):
    self._xetra_max_requests = 5
    self._xetra_duration = 2

# After discovery tests (example: found 7 req/2s is safe)
def __init__(self, base_path: Path | None = None):
    self._xetra_max_requests = 6  # 20% safety margin below 7
    self._xetra_duration = 2
```

---

## Safety Notes

⚠️ **These tests are intentionally aggressive**:
- Will make 50-100 API requests
- Will deliberately trigger 429 errors
- Takes 5-10 minutes to complete
- May temporarily block your IP from the API

✅ **Best Practices**:
- Run during off-peak hours
- Run from development machine, not production
- Wait at least 1 hour between test runs
- Only run when changing rate limit configuration
- DO NOT run in CI/CD pipeline

---

## Test Markers

These tests use two pytest markers:

```python
@pytest.mark.live  # Requires real API access
@pytest.mark.slow  # Takes several minutes
```

**Skip by default**:
```bash
# Regular test runs skip these
uv run pytest  # Skips live + slow tests
```

**Run explicitly**:
```bash
# Run only these tests
uv run pytest -m "live and slow" -v -s

# Or by filename
uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s
```

---

## Example Session

```bash
# 1. Run discovery tests
$ uv run pytest tests/test_xetra_rate_limit_discovery.py -v -s

# 2. Observe results - example:
#    ✓ 5 req/2s: 20/20 success, 0 rate limited
#    ✗ 10 req/2s: 15/20 success, 5 rate limited

# 3. Update config_service.py with safe values
$ vim src/yf_parqed/config_service.py
# Change: self._xetra_max_requests = 5  (keep default)

# 4. Run integration tests to verify
$ uv run pytest tests/test_xetra_integration.py -v

# 5. Done! Configuration validated.
```

---

## Troubleshooting

**Test fails with "Need at least X files"**:
- Testing on weekend when no trading occurred
- Run on a weekday or use historical date

**All tests hit 429 immediately**:
- May have already exhausted rate limit from previous tests
- Wait 1 hour and try again
- API may have stricter limits than expected

**Tests timeout**:
- Normal - downloads can take time
- Increase pytest timeout: `pytest --timeout=600`

**Inconsistent results**:
- Rate limits may be dynamic/time-of-day dependent
- Run multiple times and take average
- Test during same time of day for consistency

---

## Next Steps

After running discovery tests:

1. ✅ Update `ConfigService._xetra_max_requests` and `_xetra_duration`
2. ✅ Update documentation with actual limits found
3. ✅ Add results to `RATE_LIMITING_IMPLEMENTATION.md`
4. ✅ Consider adaptive rate limiting (future enhancement)
5. ✅ Monitor production logs for 429 errors
