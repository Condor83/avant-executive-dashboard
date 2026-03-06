# Avant Executive Dashboard — Golden PRD Pack

This directory contains the sprint-by-sprint PRD and the minimal set of context files to build:

- Strategy executive dashboard (ROE after fees, yield, position drilldowns)
- Market risk monitoring (kink proximity, spread compression)
- Consumer dashboard (market health, leverage %, top wallets)

## Where to start

1. `agents.md` (index — tells you what to read for your task)
2. `docs/memory.md` (non-negotiable business logic + definitions)
3. `docs/architecture.md` (system overview)
4. `docs/prd/README.md` (sprint index)

## Notes
- Strategy wallet/protocol/chain mapping lives in `markets.yaml` (provided externally in the main repo).
- Customer incentive markets are defined by Avant's Rewards page (see `docs/memory.md` for details).

## Local workflow (Sprint 01)

1. Install dependencies:
   - `uv sync --dev`
2. Set local DB environment:
   - `cp .env.example .env`
   - or export `AVANT_DATABASE_URL` and `AVANT_TEST_DATABASE_URL` explicitly
3. Start local DB + Adminer:
   - `make db-up`
4. Apply schema migrations:
   - `make db-migrate`
5. Seed dimension tables from config:
   - `make db-seed`
6. Open DB UI:
   - `make db-ui` (Adminer at `http://localhost:8080`)
7. Run manual ingestion sync:
   - `uv run python -m core.cli sync prices --as-of 2026-03-03T00:00:00Z`
   - `uv run python -m core.cli sync snapshot --as-of 2026-03-03T00:00:00Z`
   - `uv run python -m core.cli sync markets --as-of 2026-03-03T00:00:00Z`

Run checks:
- `make lint`
- `make test`

DB-backed tests use `AVANT_TEST_DATABASE_URL` to create disposable databases. CI sets
`AVANT_REQUIRE_DB_TESTS=1`, so Postgres bootstrap failures fail the suite instead of skipping it.

## Local DB Drift Policy

- The canonical schema is defined by source-controlled Alembic migrations only.
- The supported derived market table is `market_overview_daily`.
- `market_view_daily` is not part of the canonical schema and should be treated as drift if it appears in a local database.
- If a shared local DB drifts from repo head, rebuild the app database from migrations and reseed dimensions.
- Do not hand-edit `alembic_version` to paper over missing or local-only migrations.
