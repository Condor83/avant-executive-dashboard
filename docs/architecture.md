# architecture.md

## High-level system

The system has three layers:

1) **Ingestion**  
   Pulls positions + market state from on-chain RPC providers (primary) and indexed APIs (secondary), normalizes into canonical snapshots.

2) **Analytics**  
   Computes daily yield (APY-pro-rated), fee waterfall, ROE, risk signals (kink proximity, spread compression), and consumer leverage/concentration metrics.

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
derived tables (yield_daily, rollups, alerts, consumer metrics)
            |
            v
FastAPI -> Dashboard UI
```

### Why snapshot-first?

- Auditability: any number can be reproduced from stored inputs.
- Determinism: fixed block/slot snapshots allow replay.
- Works across heterogeneous protocols where tx-level accounting is expensive.

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
  - fee engine (strategy fee, Avant GOP)
  - risk engine (kink + spread compression)
  - consumer engine (wallet cohort, leverage %, top wallets)
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
