# Phase 1.5 Testing Summary: Integration & Live API Tests

**Date**: 2025-11-03  
**Status**: ✅ COMPLETE  
**Test Count**: 11 new tests (4 mocked integration + 7 live API)  
**Total Passing**: 265/265 (7 live deselected by default)

---

## Overview

Phase 1.5 implemented a dual testing strategy combining:
1. **Mocked integration tests** - Always run in CI, fast, isolated
2. **Live API tests** - On-demand only, validate against production API

This approach caught **2 critical bugs** that mocked tests alone wouldn't have found.

---

## Test Breakdown

### Mocked Integration Tests (4 tests)

**File**: `tests/test_xetra_integration.py`

| Test | Purpose | Runtime |
|------|---------|---------|
| `test_full_workflow_with_mocks` | End-to-end: list → download → parse → store → read | <1s |
| `test_multiple_files_workflow` | Combine data from 2 files | <1s |
| `test_empty_response_workflow` | Handle no files gracefully | <1s |
| `test_partial_file_failure_workflow` | Continue processing after errors | <1s |

**Total Runtime**: 1.50s  
**Always Run**: Yes (part of default test suite)

### Live API Tests (7 tests)

**File**: `tests/test_xetra_live_api.py`  
**Marker**: `@pytest.mark.live`

| Test | Purpose | Downloads |
|------|---------|-----------|
| `test_list_files_real_api` | Verify API returns file list | 0 |
| `test_download_real_file` | Download and decompress real file | 1 |
| `test_parse_real_data` | Parse production data | 1 |
| `test_full_workflow_real_api` | Full workflow: list → download → parse → store → read | 1 |
| `test_multiple_venues_real_api` | List files from 4 venues | 0 |
| `test_schema_validation_real_data` | Validate schema matches production | 1 |
| `test_historical_data_availability` | Check 7 days of historical data | 0 |

**Total Runtime**: ~5 seconds (first run), skipped on subsequent runs if rate-limited  
**Always Run**: No (deselected by default with `-m "not live"`)  
**Run With**: `uv run pytest -m live -v`

---

## Critical Bugs Found by Live API Tests

### Bug #1: Double Prefix in Filenames

**Symptom**: Downloaded files had duplicate prefixes: `DETR-posttradeDETR-posttrade-...`

**Root Cause**: API returns a `SourcePrefix` field (e.g., `DETR-posttrade-2025-11-02`) that we were concatenating with the raw filename without stripping.

**Fix**: Strip `SourcePrefix` from `CurrentFiles` before constructing final filename:

```python
source_prefix = data.get("SourcePrefix", "")
timestamp_part = raw_filename[len(source_prefix) + 1:]  # Strip prefix + dash
clean_filename = f"{prefix}-{timestamp_part}"
```

**Result**: ✅ Filenames now correctly formatted as `DETR-posttrade-2025-11-03T08_17.json.gz`

### Bug #2: Missing `/download/` in URL Path

**Symptom**: HTTP 404 errors when downloading files

**Root Cause**: Download URL was `https://mfs.deutsche-boerse.com/api/{filename}` instead of `https://mfs.deutsche-boerse.com/api/download/{filename}`

**Fix**: Add `/download/` to URL construction:

```python
url = f"{self.base_url}download/{filename}"  # Was: f"{self.base_url}{filename}"
```

**Result**: ✅ Downloads now succeed with HTTP 200

---

## API Rate Limiting Behavior

### Observed Patterns

- **Aggressive rate limiting**: HTTP 429 after ~10-12 requests in quick succession
- **No rate limit headers**: API doesn't provide `Retry-After` or `X-RateLimit-*` headers
- **Recovery time**: 30-60 seconds required between test runs
- **First run advantage**: Initial test run usually succeeds, subsequent runs get rate-limited

### Mitigation Strategy

1. **Exponential backoff retry logic** (2s, 4s, 8s delays):
   ```python
   for attempt in range(max_retries):
       try:
           response = self.client.get(url)
       except httpx.HTTPStatusError as e:
           if e.response.status_code == 429 and attempt < max_retries - 1:
               delay = base_delay * (2**attempt)
               time.sleep(delay)
               continue
   ```

2. **Sample file downloads**: Live tests now download only **1 file** instead of full day (3600+ files)
   - Before: `test_full_workflow_real_api` tried to download all 1808 posttrade + 1808 pretrade files
   - After: Downloads only `files[0]` for validation
   - Runtime: **1.56s** instead of hours

3. **List-only tests**: Several tests just list files without downloading (0 API load)

4. **Wait between runs**: Documentation warns to wait 30-60 seconds between live test runs

---

## API Response Structure

### Typical File List Response

```json
{
  "SourcePrefix": "DETR-posttrade-2025-11-02",
  "CurrentFiles": [
    "DETR-posttrade-2025-11-02-2025-11-03T08_17.json.gz",
    "DETR-posttrade-2025-11-02-2025-11-03T08_16.json.gz",
    ...
  ]
}
```

**Key Fields**:
- `SourcePrefix`: Prefix to strip from filenames (includes date)
- `CurrentFiles`: Array of raw filenames (need prefix stripping)

### File Count per Day

- **~1820 files** per venue per type per day
- Example for 2025-11-02 (DETR only):
  - 1820 posttrade files
  - 1820 pretrade files
  - **Total: 3640 files**
- 4 venues × 2 types = **~14,560 files per day across all venues**

---

## Running Tests

### Default: All Tests Except Live

```bash
uv run pytest
# Runs 265 tests, deselects 7 live tests
# Runtime: ~12 seconds
```

### Only Live API Tests

```bash
uv run pytest -m live -v
# Runs 7 live tests
# Runtime: ~5 seconds (first run), may skip if rate-limited
```

### Only Mocked Tests

```bash
uv run pytest -m "not live"
# Explicitly exclude live tests (same as default)
```

### All Tests (Including Live)

```bash
uv run pytest -m ""  # Empty marker = run all
# Or remove the `-m not live` from pyproject.toml temporarily
```

---

## Test File Organization

```
tests/
  test_xetra_fetcher.py          - XetraFetcher unit tests (19 tests)
  test_xetra_parser.py           - XetraParser unit tests (23 tests)
  test_xetra_service.py          - XetraService unit tests (14 tests)
  test_xetra_storage.py          - Partitioned storage tests (3 tests)
  test_xetra_integration.py      - Mocked integration tests (4 tests) ← NEW
  test_xetra_live_api.py         - Live API tests (7 tests) ← NEW
```

---

## Lessons Learned

1. **Live API tests are invaluable**: Found 2 critical bugs that mocked tests missed
2. **Rate limiting is real**: Production APIs have aggressive limits, design tests accordingly
3. **Sample, don't exhaust**: Downloading 1 file validates the same logic as downloading 3600
4. **Dual strategy is best**: Mocked tests for speed/CI, live tests for validation
5. **Mark expensive tests**: `@pytest.mark.live` allows selective execution
6. **Document rate limits**: Future developers need to know about wait times
7. **Exponential backoff works**: 2s/4s/8s delays handle transient 429 errors gracefully

---

## Performance Metrics

| Test Type | Count | Runtime | Downloads | Always Run? |
|-----------|-------|---------|-----------|-------------|
| Mocked Integration | 4 | 1.50s | 0 (mocked) | ✅ Yes |
| Live API | 7 | ~5s | 4 files | ❌ No (`-m live` only) |
| **Total** | **11** | **~7s** | **4** | **4 always** |

---

## Next Steps

- [x] Phase 1.5 complete
- [ ] Update `README.md` with live test usage
- [ ] Proceed to Phase 2 (ISIN Mapping)

---

## References

- Implementation Checklist: `docs/xetra/IMPLEMENTATION_CHECKLIST.md`
- Live API Tests: `tests/test_xetra_live_api.py`
- Mocked Integration: `tests/test_xetra_integration.py`
- XetraFetcher (with retry logic): `src/yf_parqed/xetra_fetcher.py`
