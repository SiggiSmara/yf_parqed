```markdown
# ADR Index

This index lists ADRs and provides a suggested classification (implemented / in-progress / to-do / idea / archived). Use the `Status:` header inside each ADR for the canonical value; this index is a convenience for discovery.

| Date | File | Title | Suggested Location | Notes |
|------|------|-------|--------------------|-------|
| 2025-10-12 | `2025-10-12-partition-aware-storage.md` | Partition-Aware Storage | implemented | ADR text says "Accepted" and release notes indicate implemented (2025-10-19).
| 2025-10-12 | `2025-10-12-duckdb-query-layer.md` | DuckDB Query Layer | idea | Roadmap lists as upcoming; likely work-in-progress.
| 2025-10-10 | `2025-10-10-yahoo-finance-data-pipeline.md` | Yahoo Finance Data Pipeline | implemented | Contains design and appears operational; release notes reference daemon mode and YF behavior.
| 2025-10-12 | `2025-10-12-xetra-delayed-data.md` | Xetra Delayed Data | implemented | Release notes: Xetra Phase 1 complete (2025-10-19). Phase 2 (OHLCV aggregation) is tracked in its own ADR.
| 2025-12-05 | `2025-12-05-ohlcv-aggregation-service.md` | OHLCV Aggregation Service | to-do | Agreed to implement (canonical aggregator). Work not started yet — suitable for 'to-do' classification.
| 2025-12-06 | `2025-12-06-separation-of-concerns.md` | Separation of Concerns | implemented | Package refactor completed; tests and import shims updated (see ADR contents).


## How to use
- If you agree with the suggested locations, I can (with your confirmation) move each ADR into the corresponding folder and update the `Status:` header inside the ADR and this index.
- New category `to-do`: indicates ADRs that have been agreed and scheduled for implementation but where work has not yet started. Use this for tracking implementation planning.
- If you'd like a different classification policy (e.g., require a linked PR to mark implemented), tell me and I'll apply that filter.


```
# ADR Index

This index lists ADRs and provides a suggested classification (implemented / in-progress / idea / archived). Use the `Status:` header inside each ADR for the canonical value; this index is a convenience for discovery.

| Date | File | Title | Suggested Location | Notes |
|------|------|-------|--------------------|-------|
| 2025-10-12 | `2025-10-12-partition-aware-storage.md` | Partition-Aware Storage | implemented | ADR text says "Accepted" and release notes indicate implemented (2025-10-19).
| 2025-10-12 | `2025-10-12-duckdb-query-layer.md` | DuckDB Query Layer | idea | Roadmap lists as upcoming; likely work-in-progress.
| 2025-10-10 | `2025-10-10-yahoo-finance-data-pipeline.md` | Yahoo Finance Data Pipeline | implemented | Contains design and appears operational; release notes reference daemon mode and YF behavior.
| 2025-10-12 | `2025-10-12-xetra-delayed-data.md` | Xetra Delayed Data | implemented | Release notes: Xetra Phase 1 complete (2025-10-19).
| 2025-10-12 | `2025-10-12-xetra-delayed-data.md` | Xetra Delayed Data | implemented | ADR: Phase 1 complete, Phase 2 & 3 (OHLCV aggregation) deferred to own ADR.
| 2025-12-05 | `2025-12-05-ohlcv-aggregation-service.md` | OHLCV Aggregation Service | in-progress | Roadmap / release notes mark OHLCV aggregation as Phase 2 (pending).
| 2025-12-06 | `2025-12-06-separation-of-concerns.md` | Separation of Concerns | implemented | Package refactor completed; tests and import shims updated (see ADR contents).


## How to use
- If you agree with the suggested locations, I can (with your confirmation) move each ADR into the corresponding folder and update the `Status:` header inside the ADR and this index.
- If you'd like a different classification policy (e.g., require a linked PR to mark implemented), tell me and I'll apply that filter.

