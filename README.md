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
2. Start local DB + Adminer:
   - `make db-up`
3. Apply schema migrations:
   - `make db-migrate`
4. Seed dimension tables from config:
   - `make db-seed`
5. Open DB UI:
   - `make db-ui` (Adminer at `http://localhost:8080`)
6. Run manual ingestion sync:
   - `uv run python -m core.cli sync prices --as-of 2026-03-03T00:00:00Z`
   - `uv run python -m core.cli sync snapshot --as-of 2026-03-03T00:00:00Z`
   - `uv run python -m core.cli sync markets --as-of 2026-03-03T00:00:00Z`

Run checks:
- `make lint`
- `make test`
