# Parquet Recovery Refactoring Summary

## Overview

Unified parquet file recovery logic across all storage backends to ensure consistent behavior:
- **Only delete truly corrupt/unreadable files** (IO errors, parse failures)
- **Preserve files with schema mismatches** for operator inspection
- **Attempt comprehensive safe recovery** before giving up

## Changes Made

### 1. New Module: `src/yf_parqed/parquet_recovery.py`

Created a shared recovery module that all storage backends use. Key features:

- `safe_read_parquet()` - Main recovery function with multi-stage process:
  1. Attempt to read the file
  2. If truly corrupt (IO/parse error) → delete and raise `ParquetRecoveryError`
  3. If empty DataFrame → preserve file and raise error
  4. If missing columns → attempt safe promotions
  5. If recovery fails → preserve file and raise error with details
  
- `_attempt_column_recovery()` - Safe column recovery strategies:
  - Promote numeric, monotonic index to 'sequence' column
  - Promote 'index' column to 'sequence' if safe
  - Skip datetime-like values and epoch-encoded timestamps
  
- `ParquetRecoveryError` - Custom exception for recovery failures

### 2. Updated `src/yf_parqed/storage_backend.py`

- Removed ~140 lines of inline recovery logic
- Now uses shared `safe_read_parquet()` function
- Catches `ParquetRecoveryError` and logs details
- Returns empty DataFrame on failure (preserving files)

### 3. Updated `src/yf_parqed/partitioned_storage_backend.py`

- Previously deleted files on any schema mismatch
- Now uses shared `safe_read_parquet()` for each partition
- Collects failures and raises detailed `RuntimeError` with all issues
- Only deletes truly corrupt files; preserves schema-mismatch files

### 4. Test Updates

Updated tests to match new behavior:

- `tests/test_storage_backend.py` - Expect files preserved
- `tests/test_partitioned_storage_backend.py` - Added test for schema preservation
- `tests/test_storage_backend_recovery.py` - Verify file preservation
- `tests/test_storage_operations.py` - Expect empty files preserved

## Behavior Changes

### Before
- **Legacy backend**: Attempted recovery, preserved schema-mismatch files
- **Partitioned backend**: Deleted files on any error (corrupt OR schema mismatch)

### After  
- **Both backends**: Use same recovery logic
- **Corrupt files** (IO/parse errors): Deleted + error raised
- **Schema mismatches**: Preserved + error raised with details
- **Empty files**: Preserved + error raised

## Benefits

1. **Consistent behavior** across all storage backends
2. **Data preservation** - operators can inspect problematic files
3. **Clear error messages** - detailed reporting of what went wrong
4. **Safer operations** - less risk of data loss
5. **Maintainable code** - single recovery implementation

## Test Results

- All 184 tests passing
- No regressions
- New behavior validated across unit, integration, and end-to-end tests

## Validation

```bash
# Run tests
uv run pytest

# Run linting
uv run ruff check . --fix
uv run ruff format .
```
