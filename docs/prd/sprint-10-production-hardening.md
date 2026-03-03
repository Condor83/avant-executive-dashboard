# Sprint 10 — Production hardening: scheduling, backfills, observability


## Outcome

Production readiness without changing business logic:

- scheduled ingestion
- backfills and reprocessing
- observability and cost controls
- stronger data quality guarantees

## Scope

### In
- Orchestration (choose one):
  - Prefect / Dagster / Airflow
- Scheduling profiles:
  - daily SOD/EOD snapshots
  - frequent market snapshots for kink monitoring
- Backfill tooling:
  - rebuild snapshots for a block range or date range
  - rebuild derived tables from snapshots
- Observability:
  - structured logs
  - run metrics (duration, failures, rows inserted)
  - alert on ingestion failure / staleness
- Cost controls:
  - RPC call batching
  - caching
  - configurable sampling frequency

### Out
- Auto-remediation/rebalancing
- Complex anomaly detection

## Deliverables

- Orchestrated flows (separate from business logic code)
- Backfill CLI:
  - `backfill snapshots --from <date> --to <date>`
  - `backfill analytics --from <date> --to <date>`
- Monitoring dashboards and alerts

## Tests (must be written and passing)

- `tests/runner/test_idempotent_snapshots.py`
  - same snapshot run twice does not duplicate rows
- `tests/backfill/test_backfill_rebuilds_rollups.py`
  - rebuild yields consistent results
- `tests/observability/test_run_metadata_recorded.py`
  - run metadata stored per job execution

## Done looks like

- [ ] Ingestion can run on a schedule without manual intervention
- [ ] Backfills can rebuild a date range deterministically
- [ ] Data freshness and coverage are monitored and alerting works
- [ ] All new tests pass in CI

## Agent start prompt

```text
Deliver Sprint 10 (production hardening).

Read:
- docs/architecture.md (runner + analytics separation)
- docs/testing.md (idempotency expectations)
- docs/lessons.md (avoid silent failures)

Implement:
- orchestration + backfills + observability + cost controls

Tests:
- idempotency + backfill + metadata recording

Avoid:
- changing business logic formulas in this sprint
```
