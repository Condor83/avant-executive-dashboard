# PRD Sprint Index

This sprint plan is designed so that after the “interface freeze” (Sprint 02) protocol adapters can be built in parallel without schema churn.

## Dependency highlights

- Sprint 00 → 01 → 02 are sequential and define the stable interfaces.
- After Sprint 02:
  - adapters can be developed in parallel
  - analytics can proceed using snapshot data
  - API/UI can proceed using mocked rollups

See `docs/dependency-order.md` for parallelization rules.

## Sprints

- Sprint 00 — Repo foundation & CI
- Sprint 01 — Canonical data model + migrations + config loaders
- Sprint 02 — Ingestion runner + pricing + wallet balances
- Sprint 03 — Aave v3 adapter (positions + market state) + golden tests
- Sprint 04 — Morpho + Euler v2 + Dolomite adapters
- Sprint 05 — Kamino (Solana) + Zest (Stacks) + Silo v2 (consumer) adapters
- Sprint 06 — Yield/fees/ROE analytics + rollups
- Sprint 07 — Risk engine (kink + spread compression) + alerting
- Sprint 08 — Market view analytics (market overview, concentration vs market totals, risk-parameter capture)
- Sprint 09 — API + exec dashboard MVP (Portfolio + Markets; consumer deferred)
- Sprint 10 — Production hardening (scheduling, backfills, observability, deterministic pipeline sequencing)
