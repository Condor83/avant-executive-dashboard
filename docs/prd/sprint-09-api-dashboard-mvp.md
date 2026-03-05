# Sprint 09 — API + Executive Dashboard MVP (Portfolio + Markets)

## Outcome

An executive-grade dashboard and API that surfaces:

- portfolio yield/ROE after fees (yesterday/7d/30d)
- fee waterfall and Avant GOP
- drilldowns by product → wallet → position
- market health, concentration, and risk watchlist
- data quality and freshness indicators

Consumer analytics is intentionally deferred until Portfolio and Markets views are stable.

## Scope

### In
- FastAPI service with endpoints:
  - `/summary`
  - `/portfolio/products`
  - `/portfolio/wallets/{wallet}`
  - `/portfolio/positions`
  - `/markets/overview`
  - `/markets/{market_id}/history`
  - `/markets/watchlist`
  - `/alerts`
  - `/data-quality`
- Dashboard UI (recommended: Next.js) with pages:
  - Executive Summary
  - Portfolio
  - Markets
  - Risk & Data Quality
- Auth:
  - basic password/OAuth (choose minimal viable)
  - role-based access (optional in v1)

### Out
- Consumer endpoints:
  - `/consumer/summary`
  - `/consumer/top-wallets`
- Consumer dashboard page and cohort analytics
- Pixel-perfect design system (iterate later)
- Public-facing product UI (this is internal/executive)

## Deliverables

- `src/api/` FastAPI app + routers
- Query layer (SQL) that reads derived tables first, then snapshots for drilldown:
  - `yield_daily`
  - `market_overview_daily`
  - `alerts`
  - `data_quality`
- `frontend/` (if using Next.js) OR documented BI tool approach
- Data quality widget:
  - last successful snapshot timestamps (positions + markets)
  - market and wallet coverage for latest run
  - recent failure count/types

## Tests (must be written and passing)

- `tests/api/test_summary_schema.py`
  - validates required KPI fields for Portfolio + Markets + data quality
- `tests/api/test_rollup_consistency.py`
  - API totals match derived DB tables
- `tests/api/test_markets_overview_consistency.py`
  - market overview endpoint totals match `market_overview_daily`
- `tests/api/test_positions_endpoint_filters.py`
  - `/portfolio/positions` supports deterministic filters/pagination
- Optional UI tests:
  - minimal Playwright smoke test: routes load and key tiles render

## Done looks like

- [ ] Exec Summary page renders with real data from DB.
- [ ] Drilldown works: product → wallet → position.
- [ ] Portfolio positions table is queryable via `/portfolio/positions`.
- [ ] Markets overview/history/watchlist are visible and consistent with `market_overview_daily`.
- [ ] Risk alerts and data quality status are visible.
- [ ] Consumer analytics is not required in Sprint 09.
- [ ] API tests pass in CI.

## Agent start prompt

```text
Deliver Sprint 09 (API + dashboard MVP for Portfolio + Markets).

Read:
- docs/memory.md (executive questions and metric definitions)
- docs/architecture.md (layers + module boundaries)
- docs/data-model.md (derived tables and snapshots)
- docs/why.md (auditability + data quality)

Implement:
- FastAPI endpoints for summary, portfolio drilldown, markets, alerts, and data quality
- Dashboard UI (or documented BI setup) focused on Portfolio + Markets clarity
- Dedicated positions endpoint (`/portfolio/positions`) for filtering/pagination use

Tests:
- API schema tests + rollup consistency + market overview consistency + positions endpoint filters

Avoid:
- recomputing yield in the API layer (read analytics tables)
- implementing consumer analytics endpoints/pages in this sprint
- hiding data quality issues; surface them prominently
```
