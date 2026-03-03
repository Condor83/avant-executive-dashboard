# Sprint 06 — Analytics: daily yield + fees + ROE + rollups


## Outcome

Daily yield and ROE metrics that executives will trust:

- daily gross yield (USD) per position/wallet/product
- fee waterfall (strategy fee, Avant GOP, net yield)
- rollups for yesterday/7d/30d
- ROE variants (gross, after firm fee, net-to-users, Avant GOP)

## Scope

### In
- Business-day (America/Denver) daily computation
- SOD/EOD snapshot selection rules
- Yield computation using pro-rated APY observed that day
- Fee waterfall application everywhere
- Rollups:
  - by wallet
  - by product
  - by protocol
  - total strategy

### Out
- Tx-level flow-adjusted PnL
- Intraday PnL (hourly)

## Deliverables

- `src/analytics/yield_engine.py`
- `src/analytics/fee_engine.py`
- `src/analytics/rollups.py`
- CLI:
  - `compute daily --date YYYY-MM-DD`
  - `compute rollups --window 7d`
- Derived tables populated:
  - `yield_daily` (position-level + wallet/product rollups)

## Tests (must be written and passing)

- `tests/analytics/test_yield_math.py`
  - synthetic snapshots with known rates
  - validates daily yield formula
- `tests/analytics/test_fee_waterfall.py`
  - strategy_fee = 0.15 * Y
  - avant_gop = 0.085 * Y
  - net_yield = 0.765 * Y
- `tests/analytics/test_timezone_business_date.py`
  - Denver midnight boundaries
  - include DST regression case
- `tests/analytics/test_rollups_consistency.py`
  - sum(position rows) == wallet rollup == product rollup within epsilon

## Done looks like

- [ ] For any date with snapshots, `compute daily` produces yield_daily rows
- [ ] Yesterday/7d/30d rollups are queryable and consistent
- [ ] Fee outputs match waterfall identities everywhere
- [ ] Tests pass in CI

## Agent start prompt

```text
Deliver Sprint 06 (analytics).

Read:
- docs/memory.md (yield definition + fees + timezone)
- docs/data-model.md (yield_daily schema)
- docs/testing.md (non-negotiable fee tests)

Implement:
- yield engine + fee engine + rollups + CLI commands

Tests:
- yield math, fee waterfall, timezone, rollup consistency

Avoid:
- adding tx-level flow accounting
- coupling analytics to specific protocols; analytics operates on canonical snapshots only
```
