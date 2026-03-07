# architecture.md

## High-level system

The system has three layers:

1) **Ingestion**  
   Pulls positions + market state from on-chain RPC providers (primary) and indexed APIs (secondary), normalizes into canonical snapshots.

2) **Analytics**  
   Computes daily yield (APY-pro-rated), fee waterfall, ROE, market-overview/concentration metrics, risk signals (kink proximity, spread compression), and (later) consumer leverage/concentration metrics.

3) **Serving (API + Dashboard)**  
   Provides executive-grade UI and drilldowns; all numbers are traceable to stored snapshots.

## Data flow (snapshot-first)

```
config (markets.yaml, wallet_products.yaml, consumer_markets.yaml)
            |
            v
ingestion runner (manual sync or scheduled)
            |
            v
raw snapshots (position_snapshots, market_snapshots, prices)
            |
            v
derived tables (yield_daily, market_overview_daily, rollups, alerts, consumer metrics)
            |
            v
FastAPI -> Dashboard UI
```

### Why snapshot-first?

- Auditability: any number can be reproduced from stored inputs.
- Determinism: fixed block/slot snapshots allow replay.
- Works across heterogeneous protocols where tx-level accounting is expensive.

## Ingestion Runtime (Current)

`SnapshotRunner` composes adapters into two ingest paths:

- Position adapters:
  - `wallet_balances`
  - `aave_v3`
  - `spark`
  - `morpho`
  - `euler_v2`
  - `dolomite`
  - `traderjoe_lp`
  - `stakedao`
  - `etherex`
  - `kamino`
  - `zest`
  - `silo_v2`
- Market adapters:
  - `aave_v3`
  - `spark`
  - `morpho`
  - `euler_v2`
  - `dolomite`
  - `kamino`
  - `zest`
  - `silo_v2`

Notes:
- `wallet_balances`, `traderjoe_lp`, `stakedao`, and `etherex` are position-only adapters.
- Ops adapters (`traderjoe_lp`, `etherex`) produce supply-side exposure snapshots and set APY fields to zero unless explicitly modeled in analytics policy.
- `stakedao` produces supply-side vault-underlying snapshots but is included in Portfolio as deployed strategy capital, grouped into a single `Curated Vault` row per configured vault.
- Current Stake DAO yield is sourced from a vault-level fixed APY override, not from on-chain APR derivation.

## Data Quality Loop

Data quality is intentionally multi-layered:

1) Ingest-time DQ rows  
   Adapter failures and missing prices are written to `data_quality` during `sync snapshot` and `sync markets`.

2) Internal coverage checks  
   `sync coverage-report` compares configured expected rows vs written rows for core lending adapters (`spark`, `morpho`, `euler_v2`, `dolomite`).

3) External reconciliation checks  
   `sync debank-coverage-audit` compares DeBank-discovered legs against DB snapshot legs for strategy wallets. This is a completeness audit, not a source-of-truth override. See `docs/debank-db-audit.md`.

## Module boundaries (suggested repo layout)

- `src/core/`
  - config loading + validation
  - chain clients (evm/solana/stacks)
  - shared types (PositionSnapshot, MarketSnapshot)
  - persistence (db access)
- `src/adapters/`
  - one folder per protocol (`aave_v3`, `morpho`, `euler_v2`, etc.)
  - must implement the adapter interface and output canonical records
- `src/analytics/`
  - yield engine (daily APY-pro-rated net interest)
  - market engine (market overview + concentration vs market totals)
  - fee engine (strategy fee, Avant GOP)
  - risk engine (kink + spread compression)
  - consumer engine (wallet cohort, leverage %, top wallets; deferred from Sprint 09 MVP)
- `src/api/`
  - FastAPI app and routes
- `frontend/` (optional; depends on stack choice)
  - executive dashboard UI

## Environments

- Local dev: docker compose Postgres + mocked keys.
- Staging/prod: Neon Postgres, hosted API, scheduled ingestion.

## Key design constraints

- Multi-chain, multi-protocol; normalize into a single schema.
- Support EVM + Solana + Stacks.
- In v1 we do not track tx-level flows; avoid building features that require it.
