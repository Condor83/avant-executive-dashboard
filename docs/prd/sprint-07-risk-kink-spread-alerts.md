# Sprint 07 — Risk engine: kink watch + spread compression + alerts


## Outcome

A market risk layer that tells executives when something needs attention:

- kink proximity watchlist
- borrow APY shock detection
- spread compression (net spread) by position
- alerts stored in DB (and optional Slack webhook)

## Scope

### In
- Risk scoring functions:
  - kink/near-kink heuristics (per protocol where possible)
  - utilization headroom
  - borrow rate delta (e.g., 1d change)
  - liquidity squeeze (available liquidity)
  - spread compression: (supply_apy + reward_apy) − borrow_apy
- Alerts engine:
  - creates rows in `alerts`
  - supports severity thresholds via config
- CLI:
  - `compute risk --date YYYY-MM-DD` (or `--as-of`)

### Out
- Automatic rebalancing recommendations (can be future)
- Sophisticated depeg/oracle risk modeling (optional later)

## Deliverables

- `src/analytics/risk_engine.py`
- `src/analytics/alerts.py`
- `config/risk_thresholds.yaml` (or similar)
- Materialized “watchlist” views/queries:
  - top markets by kink risk score
  - top positions by worst net spread

## Tests (must be written and passing)

- `tests/analytics/test_spread_compression.py`
  - synthetic positions -> verifies net spread calculation and thresholds
- `tests/analytics/test_kink_risk_scoring.py`
  - synthetic market snapshots -> verifies kink risk ordering
- `tests/analytics/test_alert_generation.py`
  - creates alert rows with correct entity references and severity

## Done looks like

- [ ] Risk engine produces ranked market and position tables from stored snapshots
- [ ] Alerts are created deterministically when thresholds are breached
- [ ] Dashboard/API can query current open alerts (even if UI not built yet)
- [ ] All tests pass in CI

## Agent start prompt

```text
Deliver Sprint 07.

Read:
- docs/memory.md (what “market risk” means in this project)
- docs/data-model.md (market_snapshots + alerts)
- docs/testing.md (deterministic tests)

Implement:
- risk scoring + alert generation + thresholds config

Tests:
- spread compression, kink scoring, alert creation

Avoid:
- protocol adapters work (risk runs on canonical snapshots)
- live-call dependencies in tests
```
