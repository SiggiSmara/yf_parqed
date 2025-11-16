# Xetra CLI Simplification (2025-11-04)

## Motivation

The original CLI interface required multiple parameters that didn't align with real-world usage patterns:

1. **--date required**: Deutsche Börse only keeps ~24 hours of data, making date mostly irrelevant
2. **--store defaulted to False**: Primary use case is storing data, not just displaying
3. **--market and --source options**: These are implicit when using `xetra-parqed` command

## Changes Made

### fetch-trades Command

**Before:**
```bash
xetra-parqed fetch-trades --venue DETR --date 2025-11-03 --store --market de --source xetra
```

**After:**
```bash
xetra-parqed fetch-trades --venue DETR
```

**Improvements:**
- `--date` now optional, defaults to today
- `--store` removed (always stores by default)
- `--no-store` added for display-only mode
- `--market` and `--source` removed from CLI (hardcoded internally as "de" and "xetra")

### list-files Command

**Before:**
```bash
xetra-parqed list-files --venue DETR --date 2025-11-03
```

**After:**
```bash
xetra-parqed list-files --venue DETR
```

**Improvements:**
- `--date` now optional, defaults to today

## Usage Examples

### Fetch and store trades (most common use case)
```bash
# Fetch today's data for Xetra
xetra-parqed fetch-trades --venue DETR

# Fetch specific date
xetra-parqed fetch-trades --venue DETR --date 2025-11-01
```

### Display-only mode (no storage)
```bash
# Just display stats without saving
xetra-parqed fetch-trades --venue DETR --no-store
```

### List available files
```bash
# List today's files
xetra-parqed list-files --venue DETR

# List specific date
xetra-parqed list-files --venue DETR --date 2025-11-01
```

### Daily data collection workflow
```bash
# Fetch all venues for today (one command each)
xetra-parqed fetch-trades --venue DETR
xetra-parqed fetch-trades --venue DFRA
xetra-parqed fetch-trades --venue DGAT
xetra-parqed fetch-trades --venue DEUR
```

## Migration Notes

### Breaking Changes
- **--store flag removed**: Old scripts using `--store` will fail
  - **Migration**: Remove `--store` from scripts (storage is now default)
  - **Display-only**: Replace `--no-store` for scripts that only displayed data

- **--market and --source removed**: These were rarely used and always had same values
  - **Migration**: Remove these flags from scripts (automatically set internally)

### Non-Breaking Changes
- **--date now optional**: Old scripts with `--date` will continue to work
- **--venue still required**: No change to venue parameter

## Test Coverage

Added comprehensive tests for new behavior:
- `test_fetch_trades_default_behavior()` - Verify storage by default
- `test_fetch_trades_no_store()` - Verify --no-store flag works
- `test_fetch_trades_default_date()` - Verify date defaults to today
- `test_list_files_default_date()` - Verify list-files date default

All 275 unit tests passing (7 live API tests excluded).

## Benefits

1. **Simpler usage**: Reduced from 5 parameters to 1 required parameter
2. **Better defaults**: Store-by-default matches primary use case
3. **Less typing**: Common workflow went from 25+ characters to 9 characters of flags
4. **Clearer intent**: Command name implies market/source, no need to specify
5. **Fewer errors**: Less required parameters means less chance of user mistakes
