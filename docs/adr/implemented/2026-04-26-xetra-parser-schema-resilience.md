# ADR 2026-04-26: Xetra Parser Schema Resilience

## Status: Implemented

## Context

In late February 2026, Deutsche Börse renamed all posttrade JSON field names as part of a MiFID-style API update (e.g., `isin` → `instrumentIdentificationCode`, `lastTrade` → `price`, `lastQty` → `quantity`). There was no prior notice.

`XetraParser` holds a single hard-coded `FIELD_MAPPING` and a `REQUIRED_FIELDS` list. When the API changed:

- Every parse call raised `ValueError: Missing required fields in trade data: isin, volume, currency, trade_time, trans_id, tick_id`
- The daemon continued running but stored zero data
- No structured alert fired — the failure was invisible until log inspection
- ~40 trading days of data (2026-03-02 → 2026-04-25) were permanently lost; the API only retains a 24-hour rolling window

The root issue is that the parser is tightly coupled to a single schema version with no detection, no fallback, and no fast-fail alerting. Any rename in `REQUIRED_FIELDS` causes a silent total blackout.

### Known schema versions

Column names are aligned with MiFIR Article 10 mandatory transparency field terminology (see [ADR 2026-04-26: Xetra Two-Tier Trade Storage](./2026-04-26-xetra-two-tier-storage.md)).

| Field (old) | Field (new 2026) | DataFrame column |
|---|---|---|
| `isin` | `instrumentIdentificationCode` | `isin` |
| `lastTrade` | `price` | `price` |
| `lastQty` | `quantity` | `quantity` |
| `currency` | `priceCurrency` | `price_currency` |
| `lastTradeTime` | `tradingDateAndTime` | `trading_date_time` |
| `transIdCode` | `transactionIdentificationCode` | `transaction_id` |
| `executionVenueId` | `venueOfExecution` | `execution_venue` |
| `distributionDateTime` | `publicationDateAndTime` | `distribution_time` |
| `tickId` | *(removed)* | *(remove)* |
| `sourceName`, `instrumentId`, `quotationType`, `tickActionIndicator`, `instrumentIdCode`, `mmtMarketMechanism`, `mmtNegotTransPretrdWaivInd` | *(all removed)* | *(remove)* |
| *(new)* | `tradingSystem` | `trading_system` |
| *(new)* | `priceNotation` | `price_notation` |
| *(new)* | `venueOfPublication` | `venue_publication` |
| `mmtTradingMode`, `mmtModificationInd`, `mmtBenchmarkRefprcInd`, `mmtPubModeDefReason`, `mmtAlgoInd` | *unchanged* | *unchanged* |

## Decision

Harden `XetraParser` with four complementary changes.

### 0. Raw-file quarantine (non-negotiable data preservation)

Any file that fails to parse **and contains actual trade data** (i.e. is valid gzip / valid JSON, but the schema is unrecognised) must be written to disk before the error propagates. Data that is never stored is permanently lost once the 24-hour API window expires.

**Storage path**: `{wrk_dir}/data/de/xetra/quarantine/{venue}/{filename}` — the original `.json.gz` bytes, untouched.

**Trigger condition**: `XetraSchemaUnknownError` is raised (see §1). Ordinary empty files, HTML error pages, and network failures are not quarantined — only files whose content is valid JSON with an unrecognised schema.

**Reprocessing**: A new CLI subcommand `xetra-parqed reprocess-quarantine {venue}` iterates the quarantine directory, attempts to parse each file with the current parser (which now contains the correct schema), stores successfully parsed files to the normal trade partition, and moves them to `quarantine/processed/` on success (or leaves them in place on continued failure). This command is idempotent and safe to run multiple times.

**Retention**: Quarantined files are kept indefinitely until explicitly cleared. No automatic pruning — they are the last copy of data that cannot be re-fetched.

**Why the 24-hour window makes this the highest-priority item**: every hour of delay between a schema change and a code fix is ~60 files permanently unrecoverable without this safety net. With quarantine in place, the fix can be deployed days later and still recover the full gap.

### 1. Multi-schema registry with auto-detection

Maintain a `SCHEMAS` registry inside `XetraParser`: a dict mapping a version string to a `FIELD_MAPPING`. At parse time, detect the active schema by checking for a sentinel field in the first raw record:

| Sentinel field present | Schema version assigned |
|---|---|
| `isin` | `"2025-legacy"` |
| `instrumentIdentificationCode` | `"2026-mifid"` |

If no sentinel matches any registered schema, raise a new `XetraSchemaUnknownError` (subclass of `ValueError`) that includes the list of actual fields received. This makes the next schema change diagnosable within one fetch cycle (≤1 min) rather than days.

### 2. Tiered required fields

Distinguish truly required fields (a trade record is worthless without them) from soft-required fields (loss of a single field is tolerable):

- **Hard-required** (drop the record if absent): `isin`, `price`, `quantity`, `trading_date_time`
- **Soft-required** (warn once per parse call, store `NaN`/`None`): `transaction_id`, `execution_venue`, `price_currency`
- **Optional** (store if present, ignore if absent): everything else

This ensures a partial schema change — e.g. only `transaction_id` is renamed — no longer causes total data loss. Affected records are stored with a null `transaction_id` and a structured `WARNING` is logged.

### 3. `schema_version` metadata column

Add a `schema_version: str` column to every stored row (value = detected version string, e.g. `"2026-mifid"`). Allows future queries to join or filter by schema era without inspecting file-level metadata. Old Parquet files will lack the column; queries must tolerate this via `union_by_name=True` and treat missing `schema_version` as `"2025-legacy"`.

## Sequenced Steps

- [x] Add `XetraSchemaUnknownError` to `src/yf_parqed/xetra/exceptions.py`
- [x] Add quarantine write to `xetra_service.py`: on `XetraSchemaUnknownError`, write raw bytes to `{wrk_dir}/data/de/xetra/quarantine/{venue}/{filename}` before re-raising
- [x] Refactor `XetraParser` to hold `SCHEMAS: dict[str, dict]` — a registry of `{version: FIELD_MAPPING}`
- [x] Implement `_detect_schema(raw_fields: list[str]) -> str` — returns version key or raises `XetraSchemaUnknownError` with the actual field list
- [x] Split `REQUIRED_FIELDS` into `HARD_REQUIRED_FIELDS` and `SOFT_REQUIRED_FIELDS`
- [x] Update `_validate_required_fields` to use tier logic: warn+null soft-required
- [x] Inject `schema_version` column in `parse()` after detection
- [x] Update `EXPECTED_DTYPES` — remove `tick_id`, add `schema_version: object`
- [x] Update `_create_empty_dataframe` and `_ensure_complete_schema` for new registry shape
- [x] Update `tests/test_xetra_parser.py` — fixtures for both schemas, schema detection tests, soft-required fallback, `schema_version` assertions
- [x] Run `uv run pytest` — 448 passed, 0 failed (2026-05-01)
- [x] Update `DATA_MODEL.md` with `schema_version` column, schema version table, and corrected column names (2026-05-01)
- [ ] ~~*(deferred)* Add `reprocess-quarantine` CLI subcommand to `xetra_cli.py`~~ **Superseded**: quarantine write path removed (write-path ADR Step B, 2026-05-01). `reprocess-raw-cache` is the functional replacement — it covers the same recovery scenario using the raw JSON cache.
- [ ] ~~*(deferred)* Add tests for quarantine write path and reprocess command~~ **Superseded**: raw cache tests in `tests/test_xetra_raw_cache.py` cover the scenario.

## Risk Controls

- Quarantine write must happen **before** the error is logged or re-raised — a crash-after-write is acceptable; a crash-before-write is not
- Quarantine directory must not be under the normal partition path to avoid accidental inclusion in DuckDB `read_parquet('**/*.parquet')` glob queries
- `reprocess-quarantine` must be idempotent: re-running it on already-processed files must be a no-op (check `quarantine/processed/` before attempting parse)
- `XetraSchemaUnknownError` must propagate up to the fetch loop and be logged as `ERROR` (not silently caught), so it appears in `journalctl` immediately on the next fetch cycle
- Schema detection sentinel selection must be deterministic; document which field is the sentinel for each version
- Old stored Parquet files (2025-legacy) lack `schema_version`; any analytics query touching both eras must use `union_by_name=True` and handle the missing column
- New schema version entries must include both `HARD_REQUIRED_FIELDS` and a complete `FIELD_MAPPING`; add a unit test that validates registry integrity at import time (no silent partial registration)

## Alternatives Considered

**Config-file driven FIELD_MAPPING** — Load the mapping from a JSON/YAML file deployable separately from the package. Rejected: adds operational surface (file must be present and consistent with the installed code), doesn't solve the alerting gap, and the field registry belongs in the codebase where it can be version-controlled and tested.

**Lossless raw-field storage** — Store all raw JSON fields as-is; run normalization as a separate offline step. Rejected for now: doubles storage complexity, breaks the "query-ready Parquet" contract, and adds an offline processing dependency. Revisit if Deutsche Börse API instability continues to increase.

**Fuzzy/semantic field matching** — Map fields by similarity heuristics (Levenshtein distance, embedding similarity). Rejected: non-deterministic and inappropriate for financial data where field identity is load-bearing.

## Consequences

- Future DB field renames are diagnosable within one fetch cycle instead of weeks
- Raw files from an unknown-schema period are fully recoverable once the parser is updated — the 40-trading-day data gap from the 2026 outage cannot happen again
- Partial schema changes (single field renamed) no longer cause total blackout
- Storage grows by one short string column per row (`schema_version`), plus quarantine disk usage during any future unknown-schema window
- Parser complexity increases: each new DB schema version adds one entry to the registry and a corresponding test fixture
- Existing 2025-legacy Parquet files remain valid and queryable unchanged
- Operators gain a `reprocess-quarantine` escape hatch that works regardless of how long the unknown-schema window lasted
