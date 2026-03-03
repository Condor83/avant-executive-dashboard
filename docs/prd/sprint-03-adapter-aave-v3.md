# Sprint 03 — Aave v3 adapter (positions + market state)


## Outcome

Full Aave v3 coverage for all configured chains in markets.yaml: position ingestion + market health snapshots.

This sprint should make Aave v3 data “trustworthy enough” to drive early executive KPIs.

## Scope

### In
- Aave v3 adapter:
  - user supplied / borrowed balances per reserve
  - normalized supply_apy / borrow_apy (variable borrow in v1)
  - optional rewards APY if incentives controller is configured
  - health factor / LTV where available
- Aave v3 market snapshots:
  - total supply, total borrow, utilization
  - supply & borrow rates
  - caps/headroom if accessible in v1
- Golden wallet test coverage across at least:
  - Ethereum
  - Avalanche
  - one L2/alt chain in config (e.g., Base)

### Out
- Aave v4 (separate later unless urgent)
- Advanced oracle reconciliation

## Deliverables

- `src/adapters/aave_v3/` adapter implementation
- Config wiring:
  - uses `pool`, `pool_data_provider`, `oracle`, and market asset list from markets.yaml
- SnapshotRunner integration:
  - adapter returns PositionSnapshot records
  - market snapshot records for each reserve

## Tests (must be written and passing)

- `tests/adapters/aave_v3/test_golden_wallets.py`
  - pick 2–3 wallets from config per chain (as available)
  - run adapter at a fixed block number using mocked JSON-RPC (or recorded responses)
  - assert:
    - at least one reserve returns expected shape
    - equity identity holds
- `tests/adapters/aave_v3/test_market_utilization.py`
  - utilization = total_borrow / total_supply within epsilon
- `tests/adapters/aave_v3/test_rate_normalization.py`
  - verifies normalization to 0.0–1.0 APY units

## Done looks like

- [ ] Running `sync snapshot` populates Aave v3 position_snapshots for configured wallets
- [ ] Running `sync markets` populates market_snapshots for configured Aave v3 reserves
- [ ] Golden wallet tests pass deterministically
- [ ] Data quality reports failures if any reserve read fails

## Agent start prompt

```text
Deliver Sprint 03 (Aave v3 adapter).

Read:
- docs/memory.md (position definitions + yield math expects supply/borrow APY)
- docs/data-model.md (required fields)
- docs/testing.md (golden wallets strategy)
- docs/lessons.md (fixed point + decimals pitfalls)

Implement:
- src/adapters/aave_v3/ fetching per-wallet positions and per-market health
- strict normalization of rates (store as 0.0–1.0 APY)

Tests:
- golden wallets + utilization + rate normalization

Avoid:
- adding new schema fields (use metadata_json if truly needed)
- mixing Aave v4 concerns into this sprint
```
