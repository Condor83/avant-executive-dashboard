# Sprint 05 — Kamino (Solana) + Zest (Stacks) + Silo v2 (consumer)


## Outcome

Non-EVM strategy coverage and consumer-market coverage:

- Kamino adapter scaffolding (Solana)
- Zest adapter (Stacks)
- Silo v2 adapter (consumer markets: market health + top holder positions)

This sprint is the bridge to full multi-chain coverage.

## Scope

### In

#### Kamino (Solana)
- Market snapshot ingestion for configured Kamino market(s)
- Adapter structure + Solana RPC client
- Position ingestion is optional in v1 if no strategy wallets are configured yet (but the adapter must support it).

#### Zest (Stacks)
- Position ingestion for configured strategy wallet(s) and markets (sBTC, aeUSDC)
- Market snapshots (total supply/borrow/utilization) if available; otherwise compute from contract read functions where possible

#### Silo v2 (consumer)
- Implement Silo v2 market health ingestion for the incentivized markets listed on the Avant Rewards page:
  - savUSD/USDC
  - savBTC/BTC.b
- Implement customer position ingestion for “top holders” (top 10–20) in those markets (only what’s needed for executive concentration checks).

> Note: Silo markets are NOT fully enumerated in markets.yaml in the active section; treat them as **consumer markets** and put their addresses in `consumer_markets.yaml`.

### Out
- DEX liquidity tasks / Pendle tasks (not required for money market health)
- Full Stacks indexing beyond what Zest needs

## Deliverables

- `src/adapters/kamino/` + `src/core/solana_client.py`
- `src/adapters/zest/` + `src/core/stacks_client.py`
- `src/adapters/silo_v2/`
- `config/consumer_markets.yaml` with at minimum:
  - chain
  - protocol
  - market address
  - collateral token(s) and borrow token(s) (or enough identifiers to query)

## Tests (must be written and passing)

- Zest:
  - golden wallet test using the configured Stacks wallet
  - decimals correctness test (sBTC 8 decimals, aeUSDC 6)
- Kamino:
  - unit tests for Solana RPC parsing
  - market snapshot schema test
- Silo v2:
  - market health snapshot test (utilization identity)
  - adapter returns correct normalized fields for a mocked market

## Done looks like

- [ ] SnapshotRunner can ingest Zest positions and write snapshots
- [ ] SnapshotRunner can ingest Kamino market snapshots (positions optional)
- [ ] SnapshotRunner can ingest Silo v2 market snapshots from consumer_markets.yaml
- [ ] All adapter tests are deterministic and pass in CI

## Agent start prompt

```text
Deliver Sprint 05.

Read:
- docs/memory.md (multi-chain + consumer market scope)
- docs/data-model.md (snapshot fields)
- docs/testing.md (deterministic strategy)

Implement:
- Zest adapter (Stacks) end-to-end
- Kamino adapter scaffolding + market snapshots
- Silo v2 consumer adapter + consumer_markets.yaml contract

Tests:
- golden tests for Zest; schema + utilization tests for Kamino/Silo

Avoid:
- inventing new schemas for Solana/Stacks — normalize into the canonical snapshot tables
- scraping the rewards page at runtime; keep consumer markets config-driven
```
