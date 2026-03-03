# Sprint 08 — Consumer analytics: cohort, leverage %, top wallets


## Outcome

Consumer-side executive visibility:

- market health for customer-exposed markets (Euler/Morpho/Silo)
- % of customers leveraged
- top wallets by borrow and risk
- concentration metrics (top 10–20)

## Scope

### In
- Customer cohort builder:
  - seed wallets from Avant asset holders (>= $50k threshold)
  - store cohort membership per business_date
- Market exposure mapping:
  - which customer wallets are exposed to which markets
- Leverage metrics:
  - leveraged = total_borrow_usd > 0
  - optional: risk tiers by health factor / LTV if available
- Top wallet lists:
  - top 10–20 by borrow_usd
  - top 10–20 by collateral_usd
  - lowest health factor
- Consumer market health tables (utilization, caps headroom, liquidity)

### Out
- Full user identity/CRM integration
- Tracking every wallet under $50k (defer)

## Deliverables

- `src/analytics/consumer_engine.py`
- `src/ingestion/customer_cohort.py` (or equivalent)
- `consumer_cohort_daily` (table or materialized view)
- Queries/endpoints for:
  - leveraged % (and trend)
  - top borrowers
  - market health by customer exposure

## Tests (must be written and passing)

- `tests/consumer/test_cohort_threshold.py`
  - wallet included iff Avant assets >= $50k (USD)
- `tests/consumer/test_debank_top_holders_vcr.py`
  - VCR cassette for DeBank token/top_holders call
- `tests/consumer/test_leverage_classification.py`
  - borrowed_usd > 0 -> leveraged
- `tests/consumer/test_top_wallets_ordering.py`
  - deterministic ordering rules

## Done looks like

- [ ] Running cohort builder produces a reproducible set of wallets for a date
- [ ] Leveraged % and top wallets can be computed from snapshots
- [ ] Consumer market health tables exist for Euler/Morpho/Silo markets
- [ ] Tests pass in CI

## Agent start prompt

```text
Deliver Sprint 08 (consumer analytics).

Read:
- docs/memory.md (consumer scope + >=$50k threshold)
- docs/data-model.md (wallets + snapshots)
- docs/testing.md (VCR for DeBank calls)

Implement:
- cohort builder (>= $50k in Avant assets)
- leverage metrics and top wallet concentration
- consumer market health rollups (Euler/Morpho/Silo)

Tests:
- threshold tests + VCR + leverage classification

Avoid:
- trying to track every wallet on-chain directly in v1
- adding new complicated indexing infra
```
