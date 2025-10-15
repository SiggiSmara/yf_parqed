# ADR 2025-10-12: Xetra Delayed Data Ingestion

## Status

Proposed (2025-10-12)

## Context

- Users would like delayed (15-minute) Xetra trading data in addition to Yahoo Finance.
- Deutsche Börse publishes freely accessible, per-minute JSON snapshots that are gzip-compressed and available for roughly 24 hours (e.g., `https://mfs.deutsche-boerse.com/api/download/DETR-posttrade-2025-10-10T21_00.json.gz`).
- The filename encodes venue (`DETR` for Xetra, `DETG` for Tradegate), feed type (`posttrade` for executed transactions), and timestamp down to the minute.
- Integrating a second data source still requires venue metadata mapping (ISIN, MIC), but avoids authentication complexity so long as we stay within the freely available window.

## Decision Drivers

1. Expand market coverage beyond US tickers.
2. Maintain historical continuity and storage guarantees provided by the existing architecture.
3. Allow incremental rollout—users may opt into Xetra data without affecting existing Yahoo workflows.
4. Minimize operational burden by leveraging delayed data (no licensing for real-time streaming).

## Proposed Approach

1. **Data Access Layer**
   - Implement an `XetraFetcher` service (or reuse a generalized HTTP fetcher) that downloads the minute-level gzip JSON files, handles decompression, and normalizes the payload into our OHLCV schema.
   - Encode the naming convention (`{venue}-posttrade-{YYYY-MM-DD}T{HH_MM}.json.gz`) so we can generate the expected URLs for each minute and venue.
   - Since the feed is anonymous, no authentication is required; add rate limiting and retries to account for short retention windows and transient failures.

2. **Schema Alignment**
   - Normalize responses to the same OHLCV schema used by `StorageBackend`; add venue metadata (e.g., `exchange` column for `XETR`, `TGAT`, etc.).
   - Convert per-trade records into minute bars (aggregate price/volume) while preserving transaction counts or trade IDs if useful.
   - Extend ticker metadata to store ISIN/WKN mappings when available to support cross-venue symbol mapping.

3. **Scheduling & CLI**
   - Introduce a dedicated scheduler loop (or extend the existing one) to backfill minute files within the retention window before they expire.
   - Add CLI commands/flags to initialize and update Xetra/Tradegate tickers separately from Yahoo tickers, including options to specify the venue and time range.
   - Ensure the migration plan for partition-aware storage takes the new source into account.

4. **Storage Strategy**
   - Align with the partition-aware ADR by using a venue-first hierarchy: e.g., `us/stocks/<interval>/...` for Yahoo data, `de/xetra/stocks/<interval>/...` for aggregated bars, and `de/xetra/trades/...` for raw transactions (with similar paths for Tradegate).
   - Generate pre-aggregated 1m/1h/1d bars during ingestion and store them under the venue-specific stock directories so current APIs remain functional.
   - Keep German-market data completely separate from Yahoo-driven directories to avoid accidental mixing; enforce strong checks so Tradegate trades never enter Xetra partitions (and vice versa).
   - Capture retention gaps: if a minute file is missing, log and surface alerts so users can decide whether to “heal” with alternative sources.

5. **Testing Plan (TDD)**
   - Start with contract tests using recorded responses or fixtures that simulate the MDS delayed data feed.
   - Add integration tests gating on environment variables to avoid real API calls during CI.
   - Ensure storage and scheduler tests cover mixed Yahoo/Xetra workloads.

6. **Documentation & Compliance**
   - Document configuration steps, licensing notes, and rate-limit expectations.
   - Provide guidance on storing credentials, and warn users to consult Deutsche Börse licensing terms before enabling the integration.

## Alternatives Considered

- **Real-time streaming**: Higher licensing cost and infrastructure requirements; deferred until delayed data pipeline proves stable.
- **Third-party aggregators**: Could simplify ingestion but reduces control and may introduce additional costs.

## Work Log

| Date       | Milestone | Status | Notes |
|------------|-----------|--------|-------|
| 2025-10-12 | ADR drafted | Pending | Awaiting prioritization. |
|            | Prototype `XetraFetcher` with fixtures | Pending | |
|            | CLI integration & scheduler wiring | Pending | |
|            | Storage validation | Pending | |
|            | Documentation & release | Pending | |

## Consequences

- Requires secure handling of new credentials and potential legal review for data usage.
- Increases operational load (monitoring, error handling) for an additional provider.
- Opens door to broader European market coverage if successful.
- Slightly higher storage footprint due to dual raw + aggregated persistence, but preserves backward compatibility until DuckDB-based analytics can take over aggregation duties.
