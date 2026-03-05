# Sprint 10 — Production hardening: scheduling, backfills, observability


## Outcome

Production readiness without changing business logic:

- scheduled portfolio and market ingestion
- deterministic backfills and reprocessing
- observability and cost controls
- stronger data quality guarantees

## Scope

### In
- Orchestration (choose one):
  - Prefect / Dagster / Airflow
- Scheduling profiles:
  - daily portfolio + market snapshots (SOD/EOD where required)
  - frequent market snapshots for risk monitoring
- Required daily execution sequence (same boundary rules as compute commands):
  1. `sync snapshot --as-of <ts>`
  2. `sync markets --as-of <ts>`
  3. `compute markets --date <denver_date>`
  4. `compute daily --date <denver_date>`
  5. `compute risk --as-of <ts>`
  6. `compute rollups --window 7d/30d`
- Backfill tooling:
  - rebuild position + market snapshots for a block/date range
  - rebuild derived tables from snapshots:
    - `market_overview_daily`
    - `yield_daily`
    - risk outputs/alerts
    - rollups
- Observability:
  - structured logs
  - run metrics (duration, failures, rows inserted, coverage)
  - alert on ingestion failure / staleness
  - market/position-specific run metrics:
    - attempted, succeeded, failed counts
    - missing price and data quality issue counts
    - rows written by table (`market_overview_daily`, `yield_daily`, `alerts`)
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
- Monitoring dashboards and alerts for freshness, coverage, and failure rates
- Run metadata capture for each scheduled/backfill job execution

## Tests (must be written and passing)

- `tests/runner/test_idempotent_snapshots.py`
  - same snapshot run twice does not duplicate rows
- `tests/backfill/test_backfill_rebuilds_rollups.py`
  - rebuild yields consistent results for market + portfolio derived tables
- `tests/observability/test_run_metadata_recorded.py`
  - run metadata stored per job execution with coverage/failure counters
- `tests/orchestration/test_daily_sequence_order.py`
  - orchestration enforces the required daily execution order

## Done looks like

- [ ] Ingestion can run on a schedule without manual intervention using the required sequence.
- [ ] Backfills can rebuild a date range deterministically for both market and portfolio analytics.
- [ ] Data freshness, coverage, and failure rates are monitored and alerting works.
- [ ] Idempotency guarantees hold for repeated snapshot and analytics runs.
- [ ] All new tests pass in CI.

## Agent start prompt

```text
Deliver Sprint 10 (production hardening).

Read:
- docs/architecture.md (runner + analytics separation)
- docs/testing.md (idempotency expectations)
- docs/lessons.md (avoid silent failures)

Implement:
- orchestration + backfills + observability + cost controls for market + portfolio pipelines
- enforce the daily run sequence:
  1) sync snapshot
  2) sync markets
  3) compute markets
  4) compute daily
  5) compute risk
  6) compute rollups

Tests:
- idempotency + backfill + metadata recording + sequence-order enforcement

Avoid:
- changing business logic formulas in this sprint
```
