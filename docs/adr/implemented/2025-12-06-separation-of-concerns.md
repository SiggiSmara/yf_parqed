# ADR: Package Separation of Concerns

## Status: Implemented (2025-12-06)

## Context

The codebase historically placed most modules under `src/yf_parqed/` without clear package boundaries. We want a stable, intention-revealing layout that isolates shared infrastructure from source-specific logic (Yahoo Finance vs Xetra) while keeping existing public import paths backward compatible during transition.

## Decision

Adopt a three-silo package layout with temporary shims for compatibility:

- `yf_parqed.common`: shared infrastructure (config, run lock, migration plan, path builder, parquet recovery, storage backends, shared types).
- `yf_parqed.yahoo`: Yahoo Finance pipeline (façade, ticker registry, interval scheduler, data fetcher).
- `yf_parqed.xetra`: Xetra pipeline (service, fetcher, parser, trading hours checker).
- Legacy top-level modules (`yf_parqed.primary_class`, `yf_parqed.storage_backend`, etc.) have been removed from the repo; external callers must import from `yf_parqed.common`, `yf_parqed.yahoo`, or `yf_parqed.xetra` directly.

## Sequenced Steps (plan vs state)

- [x] Create package folders with `__init__.py` (`common`, `yahoo`, `xetra`).
- [x] Move shared modules into `common/`; update their internal imports to `yf_parqed.common.*`.
- [x] Move Yahoo modules into `yahoo/`; update imports in YF code and CLI.
- [x] Move Xetra modules into `xetra/`; update imports in Xetra code and CLI.
- [x] Centralize shared types: move `StorageRequest`/`StorageInterface` to `common/storage.py` and update call sites.
- [x] Add a thin `rate_limiter` interface in `common/`, wrapping the existing YF limiter and Xetra burst limiter (no behavior change).
- [x] (Optional) Add `storage_router.py` in `common/` to resolve dataset + partition paths so services stop touching `_path_builder` directly.
- [x] Update `__init__.py` exports where convenient (e.g., `from .storage import StorageRequest`).
- [x] Run `uv run pytest` after refactors (last run: 2025-12-06, 402 passed after storage + rate-limiter updates).
- [x] Update architecture docs with new paths (added package layout section in `ARCHITECTURE.md`).

## Risk Controls

- Do moves in small batches (common → Yahoo → Xetra); keep daemons pointing to the same entrypoints.
- Avoid behavior changes; restrict to import-path and module-location updates.
- Preserve backward compatibility via shims until downstream importers migrate.
- Run `python -m compileall src` (or equivalent IDE checks) plus `uv run pytest` after each batch.
- Commit only after green tests for each batch.

## Consequences

- Clear ownership boundaries per data source and shared infra.
- Easier to evolve shared storage/rate-limiter abstractions without coupling source-specific logic.
- Temporary duplication (shims) until external callers update imports; plan to remove shims once downstreams are migrated.

## Follow-ups

Track remaining checklist items above; remove shims and collapse exports once callers migrate to the new package paths.

### Pending removal of legacy shims (checklist)

- [x] Rewrite internal imports to new packages (complete across src; downstreams should consume `yf_parqed.common/yahoo/xetra`).
- [x] Update tests/mocks/patch targets to new paths (all tests now point at canonical modules).
- [x] Delete shim files in `src/yf_parqed/` and re-run `uv run pytest` (402/402 passed).
- [ ] Update docs/examples referencing old module names (final sweep still pending outside `ARCHITECTURE.md`).
