# Sprint 01 — Data model + migrations + config loaders


## Outcome

A canonical database schema and config system that can load `markets.yaml` (strategy surfaces) deterministically.

This sprint is the foundation for parallel adapter work later.

## Scope

### In
- Canonical schemas + migrations (Postgres)
- Config loader/validator for:
  - `markets.yaml` (strategy protocol/chain/markets and wallets)
  - `wallet_products.yaml` (wallet → product/tranche mapping)
  - `consumer_markets.yaml` (Euler/Morpho/Silo incentive markets; can start as minimal)
- Seed/import scripts to populate dimension tables from config

### Out
- No on-chain calls yet (adapters can be stubs)
- No dashboard/UI

## Deliverables

1) **DB schema + migrations**
- dimension tables: wallets, products, chains, protocols, tokens, markets
- fact tables (empty but created): position_snapshots, market_snapshots, prices
- derived tables (optional): data_quality

2) **Config contract**
- `src/core/config.py` (pydantic models)
- strict validation with helpful error messages

3) **Seed tooling**
- `python -m src.core.seed_db --config <paths...>` populates:
  - products
  - protocol registry
  - chains registry
  - wallets (strategy wallets from markets.yaml + wallet_balances list)
  - markets and tokens

## Tests (must be written and passing)

- `tests/core/test_markets_yaml_parsing.py`
  - parses the real markets.yaml fixture
  - asserts expected top-level protocol keys exist
  - asserts each market/token has required fields (symbol/address/decimals, etc.)
- `tests/db/test_migrations_apply.py`
  - spins up local Postgres (or uses test container)
  - applies migrations cleanly
- `tests/db/test_seed_idempotent.py`
  - running seed twice does not create duplicates

## Done looks like

- [ ] Migrations apply cleanly from scratch
- [ ] Config loader validates markets.yaml and produces structured config objects
- [ ] Seed script populates dimension tables and is idempotent
- [ ] Tests in this sprint pass in CI

## Agent start prompt

```text
Deliver Sprint 01.

Read:
- docs/memory.md (wallets/products invariants)
- docs/data-model.md (canonical schemas)
- docs/testing.md (idempotency expectations)

Implement:
- DB schema + Alembic migrations
- Config parsing/validation for markets.yaml (+ stubs for wallet_products.yaml and consumer_markets.yaml)
- Seed script to load dimension tables

Tests:
- parsing + migration + idempotency tests (required)

Avoid:
- adding adapter logic (keep adapters stubbed)
- changing the config format without updating docs/data-model.md
```
