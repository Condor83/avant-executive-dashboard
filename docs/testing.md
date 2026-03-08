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
- rerunning `compute daily` for the same business date is storage-idempotent and removes stale `row_key`s
- rerunning `compute risk` for the same candidate set does not duplicate active alerts
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

### 5) Served contract audits (operational product QA)

After ingestion and reconciliation are healthy, audit the served Portfolio and Markets contracts
protocol-by-protocol.

Recommended audit flow:
1. Confirm coverage first:
   - `sync coverage-report`
   - `sync debank-coverage-audit`
2. Inspect the live served rows for one protocol at a time.
3. Verify:
   - pairing / grouping shape
   - supply-side underlying yield
   - borrow cost
   - net equity / TVL semantics
   - liquidity / utilization / kink semantics
   - whether the row is additive or monitor-only
4. Fix canonical ingestion or serving semantics before changing UI presentation.
5. Record enduring UI/API semantics in `docs/dashboard-contracts.md`.

Important:
- DeBank remains a completeness and discovery surface, not a source-of-truth override.
- Markets should be audited as pair-monitor rows, not by naively summing visible rows for reserve-style protocols.

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

## DB-backed test bootstrap

- Runtime SQLAlchemy connections use `AVANT_DATABASE_URL`.
- DB-backed tests create disposable databases from `AVANT_TEST_DATABASE_URL`.
- CI sets `AVANT_REQUIRE_DB_TESTS=1`, so Postgres bootstrap errors fail the test session.
- Local development may leave `AVANT_REQUIRE_DB_TESTS` unset to skip DB-backed tests when Postgres is unavailable.

## Shared Local DB Repair

- Treat the repo's Alembic chain as the only source of truth for shared local databases.
- If a shared local DB reports a revision that the repo does not have, do not patch `alembic_version` manually.
- If a local-only table appears that is not defined in repo migrations, treat it as drift until it is documented, migrated, and covered by tests.
- For rebuildable shared local DBs, prefer:
  - schema-only forensic snapshot
  - drop/recreate app DB
  - `alembic upgrade head`
  - `make db-seed`
- Re-run `alembic upgrade head` after rebuild to confirm the DB is at canonical head with no pending revision mismatch.
