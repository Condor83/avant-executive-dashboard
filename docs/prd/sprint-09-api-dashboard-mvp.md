# Sprint 09 — API + Executive Dashboard MVP


## Outcome

An executive-grade dashboard and API that surfaces:

- strategy yield/ROE after fees (yesterday/7d/30d)
- fee waterfall and Avant GOP
- drilldowns by product → wallet → position
- market risk watchlist and alerts
- consumer leverage and concentration

## Scope

### In
- FastAPI service with endpoints:
  - `/summary`
  - `/strategy/products`
  - `/strategy/wallets/{wallet}`
  - `/markets/watchlist`
  - `/alerts`
  - `/consumer/summary`
  - `/consumer/top-wallets`
- Dashboard UI (recommended: Next.js) with pages:
  - Executive Summary
  - Strategy Performance
  - Risk & Market Health
  - Consumer
- Auth:
  - basic password/OAuth (choose minimal viable)
  - role-based access (optional in v1)

### Out
- Pixel-perfect design system (iterate later)
- Public-facing product UI (this is internal/executive)

## Deliverables

- `src/api/` FastAPI app + routers
- Query layer (SQL) that reads derived tables (rollups) first, then snapshots for drilldown
- `frontend/` (if using Next.js) OR documented BI tool approach
- “Data quality” widget:
  - last snapshot time
  - % markets succeeded
  - # failures in last run

## Tests (must be written and passing)

- `tests/api/test_summary_schema.py`
  - validates required fields for KPI tiles
- `tests/api/test_rollup_consistency.py`
  - API totals match DB rollups
- Optional UI tests:
  - minimal Playwright smoke test: routes load and show tiles

## Done looks like

- [ ] Exec Summary page renders with real data from DB
- [ ] Drilldown works: product → wallet → position
- [ ] Risk watchlist and open alerts are visible
- [ ] Consumer leveraged % and top wallets visible
- [ ] API tests pass in CI

## Agent start prompt

```text
Deliver Sprint 09 (API + dashboard).

Read:
- docs/memory.md (executive questions and metric definitions)
- docs/architecture.md (layers + module boundaries)
- docs/data-model.md (rollups vs snapshots)
- docs/why.md (auditability + data quality)

Implement:
- FastAPI endpoints reading rollups + snapshots
- Dashboard UI (or documented BI setup) focused on clarity and drilldown

Tests:
- API schema tests + rollup consistency

Avoid:
- recomputing yield in the API layer (use analytics tables)
- hiding data quality issues; surface them prominently
```
