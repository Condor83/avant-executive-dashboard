# Sprint 02 — Ingestion runner + pricing + wallet balances


## Outcome

A working ingestion runner that can write snapshot facts (positions/markets/prices) to the database on demand (manual sync). This unlocks analytics and adapter parallelization.

## Scope

### In
- Ingestion runner framework:
  - iterates configured wallets/markets
  - calls adapters (some may be stubs at this stage)
  - persists results
  - records failures in a data_quality table
- Price service:
  - DefiLlama price fetch + caching
  - stores `prices` rows with source + timestamp
- Wallet balances ingestion:
  - reads `wallet_balances` section of markets.yaml
  - stores balances as PositionSnapshot-like rows (tagged protocol = `wallet_balances`)
- CLI commands:
  - `sync snapshot --as-of <ts>`
  - `sync markets --as-of <ts>` (market health snapshots)
  - `sync prices --as-of <ts>`
  - `compute daily --date <D>` (can be a stub that errors “not implemented” until Sprint 06)

### Out
- Full protocol adapters (those start Sprint 03+)
- Dashboard/UI

## Deliverables

- `src/core/runner.py`: SnapshotRunner
- `src/core/pricing.py`: PriceOracle (DefiLlama)
- `src/adapters/wallet_balances/`: ERC20 + native balance reads (EVM only in v1)
- Persistence layer with bulk inserts and idempotency by (as_of_ts, position_key)

## Tests (must be written and passing)

- `tests/pricing/test_defillama_prices_vcr.py`
  - uses VCR to record a price fetch and replays deterministically
- `tests/runner/test_runner_writes_rows.py`
  - uses a temporary DB
  - runs runner with a mocked adapter that returns 2 positions
  - asserts rows are inserted correctly
- `tests/adapters/wallet_balances/test_decimals.py`
  - verifies token decimals scaling (raw -> normalized)

## Done looks like

- [ ] `sync snapshot` writes at least wallet balances snapshots to DB
- [ ] `sync prices` writes token prices and can price the wallet balance tokens
- [ ] Runner does not silently skip failures; data_quality records failures per market/wallet
- [ ] All tests pass in CI

## Agent start prompt

```text
Deliver Sprint 02.

Read:
- docs/architecture.md (snapshot-first)
- docs/data-model.md (position_snapshots + prices)
- docs/testing.md (VCR + deterministic tests)

Implement:
- ingestion runner + price service + wallet balances adapter
- CLI commands for manual sync

Tests:
- VCR price test, runner DB write test, decimals test

Avoid:
- implementing protocol-specific adapters beyond wallet balances
- coupling pricing to adapters (keep pricing as a shared service)
```
