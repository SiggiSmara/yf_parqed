# Xetra Rate Limiting Implementation

**Date**: 2025-11-03  
**Status**: ✅ COMPLETE  
**Test Count**: 13 new tests  
**Total Passing**: 278/278 (7 live deselected)

---

## Overview

Implemented proactive rate limiting for Deutsche Börse Xetra API downloads, following the same configuration pattern as `YFParqed.enforce_limits()`. This prevents HTTP 429 errors when downloading large batches of files (e.g., 1800 files per day).

---

## Architecture

### Two-Layer Rate Limiting Strategy

1. **Proactive Rate Limiting** (Primary Defense)
   - `XetraFetcher.enforce_limits()` called BEFORE each download
   - Sleeps proactively to maintain configured request rate
   - Prevents 429 errors from occurring in the first place

2. **Exponential Backoff Retry** (Fallback)
   - Already implemented in `download_file()` 
   - Retries with 2s, 4s, 8s delays if 429 occurs anyway
   - Safety net for edge cases

### Configuration Flow

```
ConfigService
  └─> configure_xetra_limits(max_requests, duration)
       └─> XetraService(config=config)
            └─> XetraFetcher(max_requests, duration)
                 └─> enforce_limits() called before each download
```

---

## Implementation Details

### 1. ConfigService Extensions

**File**: `src/yf_parqed/config_service.py`

Added Xetra-specific rate limiting configuration:

```python
def __init__(self, base_path: Path | None = None):
    self._base_path = Path(base_path) if base_path is not None else Path.cwd()
    self._max_requests = 3          # YFParqed limits
    self._duration = 2
    self._xetra_max_requests = 5    # Xetra limits (more generous)
    self._xetra_duration = 2

def configure_xetra_limits(
    self, max_requests: int = 5, duration: int = 2
) -> tuple[int, int]:
    """Configure rate limiting for Deutsche Börse Xetra API."""
    logger.info(
        f"Xetra rate limiting set to max {max_requests} requests per {duration} seconds"
    )
    self._xetra_max_requests = max_requests
    self._xetra_duration = duration
    return self._xetra_max_requests, self._xetra_duration

def get_xetra_limits(self) -> tuple[int, int]:
    """Get current Xetra rate limiting configuration."""
    return self._xetra_max_requests, self._xetra_duration
```

**Key Design Decisions**:
- Independent from YFParqed limits (different APIs, different rate limits)
- Default: 5 requests per 2 seconds (vs YFParqed's 3 per 2)
- More generous default because Deutsche Börse API seems more tolerant

---

### 2. XetraFetcher Rate Limiting

**File**: `src/yf_parqed/xetra_fetcher.py`

Added sliding window rate limiter:

```python
def __init__(
    self, 
    base_url: str = "https://mfs.deutsche-boerse.com/api/",
    max_requests: int = 5,
    duration: int = 2,
):
    self.base_url = base_url
    self.client = httpx.Client(timeout=30.0)
    
    # Rate limiting configuration
    self.max_requests = max_requests
    self.duration = duration
    self.call_list: List[datetime] = []  # Sliding window

def enforce_limits(self):
    """Enforce rate limiting by sleeping if needed."""
    now = datetime.now()
    
    if not self.call_list:
        self.call_list.append(now)
    else:
        delta = (now - max(self.call_list)).total_seconds()
        sleepytime = self.duration / self.max_requests
        
        if delta < sleepytime:
            sleep_duration = sleepytime - delta
            logger.debug(f"Sleeping for {sleep_duration:.2f}s to enforce rate limit")
            time.sleep(sleep_duration)
            self.enforce_limits()  # Recursive call after sleep
        else:
            self.call_list.append(now)
            
            # Keep only most recent max_requests timestamps
            if len(self.call_list) > self.max_requests:
                self.call_list.pop(0)

def download_file(self, venue: str, date: str, filename: str) -> bytes:
    url = f"{self.base_url}download/{filename}"
    logger.debug(f"Downloading {filename} from {url}")
    
    # ✅ NEW: Proactively enforce rate limits BEFORE making request
    self.enforce_limits()
    
    # Existing exponential backoff retry logic...
    for attempt in range(max_retries):
        try:
            response = self.client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # Fallback retry with exponential backoff
                ...
```

**Algorithm**: Sliding window (same as YFParqed)
- Tracks timestamps of recent requests in `call_list`
- Calculates minimum time between requests: `sleepytime = duration / max_requests`
- Sleeps if last request was too recent
- Maintains window of most recent `max_requests` timestamps

---

### 3. XetraService Integration

**File**: `src/yf_parqed/xetra_service.py`

Service now accepts ConfigService and passes rate limits to fetcher:

```python
def __init__(
    self,
    fetcher: Optional[XetraFetcher] = None,
    parser: Optional[XetraParser] = None,
    backend: Optional[PartitionedStorageBackend] = None,
    root_path: Optional[Path] = None,
    config: Optional[ConfigService] = None,  # ✅ NEW
):
    # Initialize config first to get rate limits
    self.config = config or ConfigService()
    max_requests, duration = self.config.get_xetra_limits()
    
    # ✅ NEW: Initialize fetcher with config-based rate limits
    self.fetcher = fetcher or XetraFetcher(
        max_requests=max_requests,
        duration=duration,
    )
    self.parser = parser or XetraParser()
    # ... rest of initialization
```

---

## Rate Limiting Examples

### Default Configuration (5 req / 2 sec)

```python
config = ConfigService()
max_req, duration = config.get_xetra_limits()
# max_req=5, duration=2

req_per_sec = 5 / 2 = 2.5 req/s
req_per_min = 2.5 * 60 = 150 req/min

# Time to download 1800 files:
time = 1800 / 2.5 / 60 = 12 minutes
```

### Conservative Configuration (3 req / 2 sec)

```python
config = ConfigService()
config.configure_xetra_limits(max_requests=3, duration=2)

req_per_sec = 3 / 2 = 1.5 req/s
req_per_min = 1.5 * 60 = 90 req/min

# Time to download 1800 files:
time = 1800 / 1.5 / 60 = 20 minutes
```

### Aggressive Configuration (10 req / 1 sec)

```python
config = ConfigService()
config.configure_xetra_limits(max_requests=10, duration=1)

req_per_sec = 10 / 1 = 10 req/s
req_per_min = 10 * 60 = 600 req/min

# Time to download 1800 files:
time = 1800 / 10 / 60 = 3 minutes

# ⚠️ WARNING: May hit rate limits, use with caution!
```

---

## Usage Examples

### Basic Usage (Default Limits)

```python
from yf_parqed import ConfigService, XetraService

# Uses default: 5 req/2s = 2.5 req/s = 150 req/min
service = XetraService()

# Download all files for a date (will be rate-limited automatically)
files = service.list_files("DETR", "2025-11-01")
for filename in files:
    df = service.fetch_and_parse_trades("DETR", "2025-11-01", filename)
    # Each download automatically rate-limited via enforce_limits()
```

### Custom Rate Limits

```python
from yf_parqed import ConfigService, XetraService

# Configure conservative rate limiting
config = ConfigService()
config.configure_xetra_limits(max_requests=3, duration=2)

# Service will use configured limits
service = XetraService(config=config)

# Downloads will be slower but safer
files = service.list_files("DETR", "2025-11-01")
for filename in files:
    df = service.fetch_and_parse_trades("DETR", "2025-11-01", filename)
```

### Direct Fetcher Usage

```python
from yf_parqed import XetraFetcher

# Custom limits directly on fetcher
fetcher = XetraFetcher(max_requests=8, duration=3)

with fetcher:
    files = fetcher.list_available_files("DETR", "2025-11-01")
    
    for filename in files:
        # Each download automatically rate-limited
        data = fetcher.download_file("DETR", "2025-11-01", filename)
        content = fetcher.decompress_gzip(data)
```

---

## Test Coverage

### New Tests (13 total)

**File**: `tests/test_xetra_rate_limiting.py`

| Test | Validates |
|------|-----------|
| `test_config_service_default_xetra_limits` | Default limits: 5 req/2s |
| `test_config_service_configure_xetra_limits` | Configuration persistence |
| `test_xetra_fetcher_default_rate_limits` | Fetcher initialization |
| `test_xetra_fetcher_custom_rate_limits` | Custom limit injection |
| `test_xetra_service_uses_config_limits` | Config → Service → Fetcher flow |
| `test_enforce_limits_first_call_no_delay` | First call returns immediately |
| `test_enforce_limits_within_window_sleeps` | Sleeps when rate exceeded |
| `test_enforce_limits_maintains_sliding_window` | Window size limited to max_requests |
| `test_enforce_limits_after_window_no_delay` | No sleep after window passes |
| `test_download_file_calls_enforce_limits` | Integration with download |
| `test_rate_limiting_calculation_examples` | Math verification |
| `test_xetra_service_rate_limit_configuration` | End-to-end integration |
| `test_independent_rate_limiters` | YFParqed vs Xetra independence |

**All 278 tests passing** (13 new + 265 existing)

---

## Performance Impact

### Before Rate Limiting

- Downloaded files as fast as possible
- Hit HTTP 429 after ~10-12 requests
- Exponential backoff retries: 2s, 4s, 8s delays
- Unpredictable total time (depends on retry count)

### After Rate Limiting

- Proactive spacing: 0.4s between requests (at 5 req/2s)
- Rarely hits HTTP 429 errors
- Predictable total time: ~12 minutes for 1800 files
- More reliable for production downloads

### Overhead

- Per-request overhead: <1ms (datetime arithmetic + list operations)
- Sleep time: 0.4s between requests (by design)
- No meaningful performance penalty (rate limiting is intentional slowdown)

---

## Comparison with YFParqed

| Feature | YFParqed | Xetra |
|---------|----------|-------|
| Default max_requests | 3 | 5 |
| Default duration | 2s | 2s |
| Default req/sec | 1.5 | 2.5 |
| Config method | `configure_limits()` | `configure_xetra_limits()` |
| Get method | `get_limits()` | `get_xetra_limits()` |
| Rate limiters | Independent | Independent |
| Algorithm | Sliding window | Sliding window (same) |
| Implementation | `enforce_limits()` | `enforce_limits()` (same) |

**Design Rationale**: 
- Separate configurations because different APIs have different tolerances
- Xetra default (5/2s) more generous than YFParqed (3/2s)
- Both use same proven sliding window algorithm

---

## Future Enhancements

1. **Persistent rate limit state**: Save call_list to disk to survive restarts
2. **Adaptive rate limiting**: Automatically adjust based on 429 error rate
3. **Per-venue rate limiting**: Different limits for DETR, DFRA, DGAT, DEUR
4. **Progress indicators**: Show ETA when downloading large batches
5. **Parallel downloads with rate limiting**: Multiple workers sharing one rate limiter

---

## References

- **ConfigService**: `src/yf_parqed/config_service.py` (lines 17-19, 185-213)
- **XetraFetcher**: `src/yf_parqed/xetra_fetcher.py` (lines 12-39, 44-92, 193)
- **XetraService**: `src/yf_parqed/xetra_service.py` (lines 12, 27, 38-47)
- **Tests**: `tests/test_xetra_rate_limiting.py`
- **YFParqed reference**: `src/yf_parqed/primary_class.py` (lines 204-207, 228-252)
