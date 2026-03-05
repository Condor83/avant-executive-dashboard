# Sprint 08 — Market View: market overview + portfolio concentration

## Outcome

A durable, executive-grade Market view that answers:

- how healthy are the markets Avant is exposed to?
- where is Avant most concentrated vs total market size?
- which markets are becoming fragile due to utilization, liquidity, or rate spread pressure?

This sprint powers the `Markets` view in the new dashboard framing:

- `Portfolio`
- `Markets`
- `Consumer`

## Scope

### In

- Reuse existing protocol market adapters (`collect_markets`) and extend where needed.
- Persist market-level risk parameters as explicit columns on `market_snapshots`:
  - `max_ltv`
  - `liquidation_threshold`
  - `liquidation_penalty`
- Build a deterministic market analytics layer from stored snapshots:
  - total supply, total borrow, utilization, available liquidity
  - supply APY, borrow APY, spread APY
  - Avant concentration vs market totals (supply share, borrow share)
- Build daily market overview rows from the latest common snapshot timestamp
  (market + position) inside each Denver business day.
- Add CLI compute command for market overview generation (pre-API).

### Out

- Consumer cohort metrics, leveraged wallet %, top-wallet ranking.
- Top-holder ingestion expansion.
- FastAPI endpoints and dashboard route wiring (Sprint 09).
- Rework of portfolio/yield math already delivered in Sprints 06-07.

## Public Interfaces and Data Contracts

- `market_snapshots` new nullable columns:
  - `max_ltv` (0.0-1.0)
  - `liquidation_threshold` (0.0-1.0)
  - `liquidation_penalty` (0.0-1.0)

- `MarketSnapshotInput` adds matching optional fields.

- Protocol extraction policy:
  - Aave v3 / Spark: populate all three fields when reserve configuration data is available.
  - Morpho: populate `max_ltv` from LLTV when available; leave liquidation fields null unless a canonical value is available.
  - Euler v2, Dolomite, Kamino, Zest, Silo v2: populate fields when canonical values are available from current adapter surfaces; otherwise null.
  - Null is valid and must not fail ingest.

- Derived market overview metrics:
  - `utilization = total_borrow_usd / total_supply_usd` (0 when supply is 0)
  - `available_liquidity_usd = max(total_supply_usd - total_borrow_usd, 0)`
  - `spread_apy = supply_apy - borrow_apy`
  - `avant_supplied_usd = sum(position_snapshots.supplied_usd by market_id at as_of)`
  - `avant_borrowed_usd = sum(position_snapshots.borrowed_usd by market_id at as_of)`
  - `avant_supply_share = avant_supplied_usd / total_supply_usd` (null when supply is 0)
  - `avant_borrow_share = avant_borrowed_usd / total_borrow_usd` (null when borrow is 0)

## Deliverables

- Migration for new `market_snapshots` columns.
- Updated models/types/runner persistence for new fields.
- Adapter updates for market risk-parameter extraction.
- `src/analytics/market_engine.py`
- CLI:
  - `compute markets --date YYYY-MM-DD`
- Derived table:
  - `market_overview_daily`

## Tests (must be written and passing)

- `tests/db/test_migrations_apply.py`
  - verifies new `market_snapshots` columns exist.
- `tests/runner/test_runner_writes_market_rows.py`
  - verifies new risk-parameter columns persist.
- `tests/adapters/aave_v3/test_market_risk_params.py`
  - verifies max LTV, liquidation threshold, liquidation penalty normalization.
- `tests/adapters/spark/test_market_risk_params.py`
  - verifies max LTV, liquidation threshold, liquidation penalty normalization.
- `tests/adapters/morpho/test_market_risk_params.py`
  - verifies LLTV mapping to `max_ltv` and null policy for unavailable liquidation fields.
- `tests/analytics/test_market_overview_engine.py`
  - verifies metric formulas, denominator-null handling, and source-priority selection.
- `tests/analytics/test_market_concentration.py`
  - verifies concentration metrics using summed positions per market.

## Done looks like

- [ ] `sync markets` writes market snapshots with new risk-parameter columns.
- [ ] Market analytics computation produces deterministic `market_overview_daily` rows.
- [ ] Concentration and market-health metrics are reproducible from snapshots.
- [ ] Sprint 08 has no consumer analytics DoD items.
- [ ] All tests pass in CI.

## Agent start prompt

```text
Deliver Sprint 08 (Market view).

Read:
- docs/memory.md
- docs/data-model.md
- docs/testing.md
- docs/architecture.md

Implement:
- explicit market risk columns on market_snapshots
- adapter-level market risk parameter extraction (reuse existing adapters)
- market overview analytics engine + compute CLI

Tests:
- migration, runner persistence, adapter risk params, market overview math/concentration

Avoid:
- consumer cohort/top-wallet analytics in this sprint
- API route/UI changes (Sprint 09)
- schema churn outside the defined market snapshot additions
```
