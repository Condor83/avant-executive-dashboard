# Avant Executive Dashboard — Golden PRD Pack

This directory contains the sprint-by-sprint PRD and the minimal set of context files to build:

- Strategy executive dashboard (ROE after fees, yield, position drilldowns)
- Market risk monitoring (kink proximity, spread compression)
- Consumer dashboard (market health, leverage %, top wallets)

## Where to start

1. `docs/agents.md` (index — tells you what to read for your task)
2. `docs/memory.md` (non-negotiable business logic + definitions)
3. `docs/architecture.md` (system overview)
4. `docs/prd/README.md` (sprint index)

## Notes
- Strategy wallet/protocol/chain mapping lives in `markets.yaml` (provided externally in the main repo).
- Customer incentive markets are defined by Avant's Rewards page (see `docs/memory.md` for details).
