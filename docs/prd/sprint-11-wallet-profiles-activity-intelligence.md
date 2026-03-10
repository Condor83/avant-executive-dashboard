# Sprint 11 — Strategy wallet profiles + activity intelligence

## Outcome

An executive-grade profile page for each live strategy wallet that combines current economics,
wallet-level performance history, and tx-derived operational activity so Avant can evaluate
strategist effort, execution drag, and financial transparency without redefining canonical yield.

## Scope

### In
- Internal wallet profile route linked from the Wallets page
- Wallet-level current summary:
  - TVL
  - supply
  - borrow
  - live position count
  - product assignment
- Wallet-level history for trailing 90 days:
  - supply
  - borrow
  - TVL
  - gross yield
  - net yield
  - tx count
  - gas fees USD
  - swap slippage USD
- New wallet activity ingestion for configured strategy wallets only
- Normalized wallet operation taxonomy:
  - `supply`
  - `withdraw`
  - `borrow`
  - `repay`
  - `swap`
  - `claim`
  - `bridge`
  - `approve`
  - `other`
- Executive activity rollups:
  - txs per day/week/month
  - active days
  - failed tx rate
  - distinct protocols/chains touched
  - gas per $1m average equity
  - execution drag as % of gross yield
  - swap slippage coverage
- Recent operations table with tx hash links
- Wallet detail API response schemas and frontend types
- Wallet-context alert support where available

### Out
- Full per-position operational cost accounting
- Consumer wallet detail pages
- Product/user deposit-withdraw flow attribution
- Recomputing canonical yield/ROE to include gas or slippage
- Peer-ranking framework beyond capital-normalized wallet metrics

## Deliverables

1. **Wallet detail API**
- add `GET /wallets/{wallet_address}?days=90`
- response should include:
  - `wallet`
  - `history`
  - `activity_summary`
  - `positions`
  - `operations`
  - `alerts`

2. **Wallet activity data model**
- raw wallet tx facts keyed by wallet + chain + tx hash
- normalized wallet operations keyed by tx hash + operation index
- daily wallet activity rollups keyed by Denver business date + wallet

3. **Wallet profile UI**
- Wallets table links internally to the wallet profile
- wallet profile header KPIs:
  - current TVL
  - current supply
  - current borrow
  - 30D gross yield
  - 30D net yield
  - 30D execution drag
  - 30D txs per $1m average equity
- history visualization for economics + activity
- current positions table reusing existing portfolio row contract
- recent operations table

4. **Metric semantics**
- existing `YieldDaily.wallet_id` rollups remain the canonical wallet profitability source
- gas fees and swap slippage are supplemental operational transparency only
- activity must be judged with capital-normalized metrics, not raw counts alone
- slippage is only reported for swap-like operations with deterministic coverage; uncovered cases
  remain null and are excluded from totals

## Tests (must be written and passing)

- `tests/analytics/test_wallet_activity_normalization.py`
  - classifies normalized operation kinds correctly
  - handles multi-op txs
  - handles approvals-only txs
  - handles failed txs
  - handles swaps with and without slippage coverage
- `tests/analytics/test_wallet_activity_daily_rollups.py`
  - Denver day boundary behavior
  - idempotent recompute/backfill behavior
  - correct gas/slippage aggregation
  - correct per-$1m normalization
- `tests/api/test_wallet_detail_endpoint.py`
  - schema and empty-state behavior
  - wallet not found behavior
  - 90-day history payload shape
- `tests/frontend/test-wallet-profile-page.tsx`
  - Wallets page links to wallet profile
  - wallet detail loading/error states
  - KPI rendering
  - recent operations rendering
- golden-wallet integration coverage for a fixed set of strategy wallets with recorded tx history

## Done looks like

- [ ] Wallets page links to an internal wallet profile
- [ ] Wallet profile loads current wallet economics and 90D history
- [ ] Activity rollups populate from normalized tx ingestion for strategy wallets
- [ ] Gas and swap slippage are visible as supplemental execution-drag metrics
- [ ] Canonical yield, fee waterfall, and ROE semantics remain unchanged
- [ ] Tests for normalization, rollups, API, and frontend pass in CI

## Agent start prompt

```text
Deliver Sprint 11.

Read:
- docs/memory.md
- docs/architecture.md
- docs/dashboard-contracts.md
- docs/data-model.md
- docs/testing.md

Build:
- strategy wallet profile pages linked from Wallets
- wallet detail API
- tx-derived wallet activity ingestion + normalized operation rollups
- 90-day wallet economics + activity history
- executive activity and execution-drag metrics

Important:
- keep wallet economics anchored to existing snapshot/yield contracts
- do not redefine canonical yield/ROE with gas or slippage
- keep v1 wallet-first; per-position effort attribution is out of scope
- align all daily rollups to America/Denver business dates

Tests:
- add unit, DB-backed, API, frontend, and golden-wallet coverage for the new wallet activity
  layer and wallet detail page
```
