# ADR 2026-04-26: Xetra Two-Tier Trade Storage

## Status: To-Do

## Context

See [ADR 2026-04-26: Xetra Parser Schema Resilience](./2026-04-26-xetra-parser-schema-resilience.md) for the incident that motivates this decision. In summary: Deutsche Börse renamed all posttrade JSON field names without notice in late February 2026, causing a ~40 trading day data gap. The parser hardening ADR addresses detection and quarantine. This ADR addresses the underlying storage architecture that made a silent total failure possible in the first place.

The root cause from a storage perspective: all fields — whether mandated by EU regulation or vendor-specific implementation detail — are stored in the same flat Parquet schema under a single data contract. When any field changes, the entire contract is invalidated. There is no stable core that downstream consumers can rely on regardless of vendor decisions.

### Why the core is stable by law

MiFIR Article 10 mandates that post-trade transparency publications for equity instruments must include a specific set of fields. These fields are defined by EU regulation and enforced by ESMA. Deutsche Börse cannot remove or fundamentally alter them without violating MiFIR. The 2026 rename was from Deutsche Börse's internal names *to* the ESMA-defined field names — the data itself did not change, only the JSON keys. This gives us a principled, regulation-backed definition of the minimum contract.

## Decision

Split Xetra trade storage into two tiers with independent schema contracts.

### Tier 1 — Core trade record (stable contract)

**Path**: `{wrk_dir}/data/de/xetra/trades/{venue}/date={date}/trades_core.parquet`

Seven columns derived from the MiFIR Article 10 mandatory transparency fields. Column names use snake_case forms of the ESMA Article 10 field terminology. Schema is frozen: no columns are ever added or removed from this tier without a new ADR.

| Column | Type | Source field (new) | Source field (legacy) | MiFIR Article 10 field |
|---|---|---|---|---|
| `isin` | `object` | `instrumentIdentificationCode` | `isin` | Instrument identification code |
| `trading_date_time` | `datetime64[ns]` | `tradingDateAndTime` | `lastTradeTime` | Trading date and time (UTC, tz-stripped) |
| `price` | `float64` | `price` | `lastTrade` | Price |
| `quantity` | `float64` | `quantity` | `lastQty` | Quantity |
| `price_currency` | `object` | `priceCurrency` | `currency` | Price currency (ISO 4217) |
| `execution_venue` | `object` | `venueOfExecution` | `executionVenueId` | Venue of execution (MIC code) |
| `transaction_id` | `object` | `transactionIdentificationCode` | `transIdCode` | Transaction identification code — dedup key + join key to Tier 2 |

Downstream consumers (OHLCV aggregation, price analysis, screening) read only this tier. They are fully insulated from vendor schema changes.

### Tier 2 — Extended metadata (flexible contract)

**Path**: `{wrk_dir}/data/de/xetra/trades_ext/{venue}/date={date}/trades_ext.parquet`

All remaining fields plus `transaction_id` as the join key. Schema evolves freely: new columns appear as Deutsche Börse adds them; removed columns simply disappear. Consumers of this tier must always use `union_by_name=True` and treat any column as nullable.

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | `object` | Join key back to Tier 1 |
| `distribution_time` | `datetime64[ns]` | Publication lag analysis |
| `trading_system` | `object` | 2026-mifid only |
| `price_notation` | `object` | 2026-mifid only |
| `venue_publication` | `object` | 2026-mifid only |
| `trading_mode` | `object` | MMT field, both schemas |
| `modification_flag` | `object` | MMT field, both schemas |
| `benchmark_flag` | `object` | MMT field, both schemas |
| `pub_deferral` | `object` | MMT field, both schemas |
| `algo_indicator` | `bool` | MMT field, both schemas |
| `schema_version` | `object` | e.g. `"2026-mifid"` |
| `message_id` | `object` | legacy schema only |
| `tick_id` | `Int64` | legacy schema only |
| `source_name` | `object` | legacy schema only |
| *(future fields)* | — | land here automatically |

### Join pattern

```sql
SELECT c.isin, c.trading_date_time, c.price, c.quantity, e.trading_mode
FROM read_parquet('trades/**/trades_core.parquet') c
JOIN read_parquet('trades_ext/**/trades_ext.parquet', union_by_name=true) e
  USING (transaction_id)
```

Tier 1 queries never need `union_by_name=True` — the schema is frozen.

### Quarantine interaction

When the parser raises `XetraSchemaUnknownError` (see parser resilience ADR), the raw file is quarantined. On `reprocess-quarantine`:

- If core fields are extractable: write to Tier 1 immediately; write remaining fields to Tier 2.
- If core fields are not extractable (no sentinel match at all): leave in quarantine, log ERROR.

This means even during an unknown-schema window, any file where the core fields can be identified (even by heuristic position or partial mapping) yields useful Tier 1 data.

## Sequenced Steps

- [ ] Update `XetraParser.parse()` to return `(core_df, ext_df)` tuple instead of a single DataFrame
- [ ] Update `XetraService` (or equivalent) to write `core_df` to `trades/` path and `ext_df` to `trades_ext/` path
- [ ] Update `PartitionPathBuilder` (or storage router) to resolve both tier paths from `(venue, date)`
- [ ] Update `reprocess-quarantine` CLI command to split output into both tiers
- [ ] Add `migrate-to-two-tier` CLI command: reads existing single-tier `trades/` Parquets, splits into core + ext, writes to new paths; safe to run multiple times (skips already-migrated dates)
- [ ] Update `DATA_MODEL.md` with two-tier schema, join pattern, and column ownership table
- [ ] Update `tests/test_xetra_parser.py` — assert `parse()` returns a tuple; assert core DataFrame has exactly 7 columns; assert ext DataFrame contains `transaction_id`
- [ ] Update `tests/test_xetra_service.py` — assert writes go to both paths
- [ ] Run `uv run pytest` — all tests must pass

## Risk Controls

- `transaction_id` must be present in both tiers; if a record has a null `transaction_id`, it must still be stored but logged as a WARNING (left-join semantics: the core record is not dropped)
- `migrate-to-two-tier` must be idempotent — check for existence of `trades_core.parquet` before processing a date
- Tier 1 path (`trades/`) must remain stable after migration; existing DuckDB queries that glob `trades/**/*.parquet` will start reading core-only files — verify they don't rely on any column that moved to Tier 2
- Do not mix old single-tier files and new core files in the same partition directory; migration moves the old file to `trades_legacy/` before writing the new split

## Alternatives Considered

**JSON blob column for extended fields** — Store core columns + a `raw_extra: str` (JSON string of all non-core fields) in a single file. Rejected: DuckDB cannot efficiently filter or aggregate on JSON blob columns; loses type information; doesn't actually stabilise the core schema.

**Single file with clear column grouping but no physical split** — Keep one Parquet file but document which columns are "core" vs "extended" in the data model. Rejected: doesn't enforce the contract; a schema change still invalidates the whole file; downstream consumers cannot trust the schema without reading all columns.

**Retroactive backfill of Tier 2 from quarantine** — Store only Tier 1 during unknown-schema windows; reconstruct Tier 2 from quarantined files when the parser is fixed. Accepted as a fallback: this is exactly what `reprocess-quarantine` enables. Tier 1 is the safety net; Tier 2 is best-effort.

## Consequences

- OHLCV aggregation and all core analytics are permanently insulated from vendor schema changes
- Future Deutsche Börse field additions land in Tier 2 automatically with no code change required
- Tier 1 schema changes require a new ADR by convention — enforces deliberate governance
- Storage roughly doubles for extended metadata, though Tier 2 files compress well (mostly short strings and flags)
- Queries requiring both core and extended fields require an explicit join on `transaction_id`
- Existing 2025-legacy single-tier files must be migrated or left under `trades_legacy/` — mixed state is temporary until `migrate-to-two-tier` is run
- **OHLCV aggregation is responsible for translating MiFIR column names to finance conventions**: Tier 1 uses `quantity`, `trading_date_time`, `execution_venue`, `price_currency`, `transaction_id` (MiFIR Article 10 terminology). The OHLCV aggregator must rename these explicitly — `quantity → volume`, `trading_date_time → timestamp` (or the interval bucket label) — as part of its transformation step. The raw tier speaks MiFIR; the derived tier speaks finance. See [OHLCV Aggregation Service ADR](../to-do/2025-12-05-ohlcv-aggregation-service.md).
