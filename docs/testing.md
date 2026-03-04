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
- `sync markets` writes rows to `market_snapshots`
- `compute daily` writes rows to `yield_daily`
- API endpoints return JSON with expected schema
- `compute boundary-check --date <D>` reports `status=pass` only when exact Denver SOD/EOD snapshots exist
- In dev/testing, `compute daily --boundary-policy latest_snapshot` may use the latest available snapshot for both SOD/EOD as an approximation; those rows should be treated as non-signoff metrics.

### 4) Coverage reconciliation audits (operational test layer)

These are read-only validation steps used during live QA and release checks.

- Internal adapter coverage:
  - run `sync coverage-report --as-of <ts>`
  - this command currently evaluates configured-vs-written coverage for `spark`, `morpho`, `euler_v2`, and `dolomite`
- External completeness check:
  - run `sync debank-coverage-audit --as-of <ts> --output-json <path>`
  - compare DeBank-discovered legs vs DB legs for strategy wallets
  - treat DB/RPC as canonical for analytics; DeBank is a reconciliation surface, not a source-of-truth override

Recommended reconciliation flow:

1. Run `sync snapshot` and `sync markets` at the target as-of time.
2. Run `sync coverage-report` to catch obvious ingest failures quickly.
3. Run `sync debank-coverage-audit` and triage top unmatched USD legs.
4. Resolve high-USD configured-surface misses first (config gaps, account ids/numbers, adapter parsing).
5. Classify non-config mismatches (ops exposures, reward-only protocols, DeBank token-label semantics).

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
- ROE denominator policy:
  - ROE variants are null when avg_equity_usd <= 0
  - rollup ROE uses ratio-of-sums (not average of row ROEs)
- Reconciliation policy:
  - DeBank-vs-DB audit logic must be deterministic for a fixed DB snapshot and DeBank payload set
  - token canonicalization and manual override paths must be unit-tested when changed

## “Done” means tests pass in CI

Every sprint defines which test suites must be added or extended. CI must run:
- lint/format checks
- unit tests
- adapter golden tests (can be marked as “integration” and run in a separate job)
