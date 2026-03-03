# testing.md

## Test layers

### 1) Unit tests (fast, pure)
- Config parsing + validation
- Rate normalization (APR/APY conversions, fixed-point units)
- Yield and fee math
- Risk scoring functions (kink proximity, spread compression)

### 2) Adapter “golden wallet” tests (deterministic integration)
For each protocol adapter:
- Pick 2–5 wallet addresses from config as fixtures.
- Record RPC responses at a fixed block/slot (or use mocked responses).
- Assert:
  - returned positions are non-empty when expected
  - decimals are correct (amounts scale properly)
  - invariants hold:
    - supplied_usd >= 0
    - borrowed_usd >= 0
    - equity_usd = supplied_usd − borrowed_usd (within tolerance)
    - utilization is within [0, 1.5] (guardrail; some protocols can exceed 1 due to accounting quirks)

### 3) End-to-end smoke tests
- `sync snapshot` writes rows to `position_snapshots`
- `compute daily` writes rows to `yield_daily`
- API endpoints return JSON with expected schema

## External call recording

- Use `vcrpy` for HTTP APIs (DeBank, DefiLlama).
- For EVM RPC:
  - either mock `web3` calls at the JSON-RPC layer
  - or record JSON-RPC with a cassette system (advanced; optional)

## Non-negotiable tests for yield

- Fee waterfall identity:
  - strategy_fee = 0.15 * gross_yield
  - avant_gop = 0.085 * gross_yield
  - net_yield = 0.765 * gross_yield
- Timezone boundary correctness (Denver):
  - business_date mapping for UTC timestamps
  - DST regression test

## “Done” means tests pass in CI

Every sprint defines which test suites must be added or extended. CI must run:
- lint/format checks
- unit tests
- adapter golden tests (can be marked as “integration” and run in a separate job)
