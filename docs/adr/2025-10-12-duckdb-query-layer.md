# ADR 2025-10-12: DuckDB Query Layer

## Status

Proposed (2025-10-12)

## Context

- Analysts want ad-hoc queries across historical OHLCV data without exporting to external warehouses.
- Partition-aware storage (see ADR 2025-10-12) will create a directory layout well-suited to DuckDB and other engines.
- Current workflows require custom scripts to aggregate parquet files, slowing investigation and validation.

## Decision Drivers

1. Provide a fast, zero-copy analytics option using embedded DuckDB.
2. Avoid duplicating storage; reuse parquet files already written by the update pipeline.
3. Keep the CLI and library ergonomics simple for non-analyst users.
4. Minimize additional dependencies for users who do not need analytics features.

## Proposed Approach

1. **Optional Dependency**
   - Add DuckDB as an optional extra (`pip install yf_parqed[duckdb]`).
   - Guard imports so the base package remains usable without DuckDB installed.

2. **Query Service**
   - Introduce a `QueryService` (or similar) that encapsulates DuckDB connections and SQL execution.
   - Provide high-level helpers (e.g., `select_prices(ticker, interval, start, end)`) built on top of parametrized SQL templates scanning partitioned parquet files.

3. **CLI Integration**
   - Extend the Typer CLI with commands like `query prices --ticker AAPL --interval 1d --sql "..."` for quick exploration.
   - Support output formats: table preview, CSV export, optional DuckDB table snapshots.

4. **Testing Plan (TDD)**
   - Start with contract tests that mock DuckDB to assert SQL generation and file globbing.
   - Add integration tests behind a DuckDB marker that run against temporary partitioned data (skip if DuckDB extras are missing).
   - Ensure the CLI command has coverage for argument parsing, output formatting, and error handling.

5. **Documentation**
   - Document installation of the DuckDB extra, sample queries, and performance guidance.
   - Cross-reference partition-aware storage requirements; warn that the analytics layer expects the new directory layout.

## Alternatives Considered

- **Polars/DataFrame API**: Offers similar capabilities but requires loading data into memory, while DuckDB can stream from disk.
- **External warehouse**: Offloads computation but introduces latency, cost, and synchronization complexity.

## Work Log

| Date       | Milestone | Status | Notes |
|------------|-----------|--------|-------|
| 2025-10-12 | Draft ADR | Pending | Initial plan captured; blocked until partition-aware storage lands. |
|            | Optional dependency wiring | Pending | |
|            | Query service implementation | Pending | |
|            | CLI + integration tests | Pending | |
|            | Docs & examples | Pending | |

## Consequences

- Increases optional dependency surface; must ensure base installation remains lightweight.
- Encourages adoption of partition-aware storage to unlock analytics features.
- Requires monitoring for long-running queries; may need future work on resource limits and caching.
