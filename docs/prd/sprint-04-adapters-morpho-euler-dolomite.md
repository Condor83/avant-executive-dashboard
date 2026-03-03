# Sprint 04 — Morpho + Euler v2 + Dolomite adapters


## Outcome

EVM “core strategy” adapters beyond Aave:

- Morpho (Blue markets; plus optional MetaMorpho vault exposure tracking)
- Euler v2 vault positions
- Dolomite margin positions

These three, combined with Sprint 03, should cover the majority of strategy positions listed in markets.yaml.

## Scope

### In

#### Morpho adapter (Blue)
- For each configured chain:
  - read Morpho core contract address from config
  - fetch per-wallet supply/borrow by market id
  - normalize:
    - supplied_amount/usd
    - borrowed_amount/usd
    - supply_apy / borrow_apy (and reward_apy if modeled)
- Market snapshots:
  - total supply, total borrow, utilization, rates (where available)

#### Euler v2 adapter
- For each configured vault address:
  - read per-wallet supply/borrow
  - normalize rates and USD values
- Market snapshots per vault (TVL, total borrows, utilization)

#### Dolomite adapter
- Read account balances by market id from the margin contract
- Normalize per-asset supply/borrow legs into canonical PositionSnapshots

### Out
- Perfect IRM parameter extraction for kink modeling (that’s Sprint 07)
- Complex vault share accounting beyond what’s needed for correct balances

## Deliverables

- `src/adapters/morpho/`
- `src/adapters/euler_v2/`
- `src/adapters/dolomite/`
- Adapter integration into SnapshotRunner
- “Coverage report” script that shows what % of wallets/markets succeeded

## Tests (must be written and passing)

For each adapter:
- `tests/adapters/<protocol>/test_golden_wallets.py`
  - at least 2 wallets from markets.yaml (per chain where possible)
  - deterministic mocked responses or recorded calls
- `tests/adapters/<protocol>/test_invariants.py`
  - supplied_usd >= 0, borrowed_usd >= 0
  - equity_usd = supplied_usd − borrowed_usd (epsilon)
  - utilization bounds sanity check
- If Morpho includes defillama_pool_id:
  - test that the adapter attaches it for downstream yield fallback logic

## Done looks like

- [ ] `sync snapshot` writes Morpho/Euler/Dolomite position snapshots for config wallets
- [ ] `sync markets` writes market snapshots for those markets/vaults
- [ ] Golden wallet tests pass for each adapter
- [ ] Coverage report shows:
  - which wallets failed and why
  - which markets are missing prices

## Implementation notes (March 2026)

- Implemented adapters:
  - `src/adapters/morpho/`
  - `src/adapters/euler_v2/`
  - `src/adapters/dolomite/`
- Runner integration:
  - `sync snapshot` + `sync markets` ingest all three adapters.
  - Coverage report command is available via `sync coverage-report`.
- Morpho APY policy implemented:
  - market snapshot rates remain protocol-native Morpho Blue rates.
  - position `supply_apy` can use DefiLlama collateral carry APY when `defillama_pool_id` is configured.
  - `borrow_apy` remains protocol-native.
- Shared DefiLlama yields utility is in place for cross-adapter reuse (`src/core/yields.py`), now used by both Aave and Morpho.
- Live ingestion checks run for one wallet/chain per protocol and full Morpho coverage; no protocol-specific ingestion blockers were found in those runs.
- Known non-blocking noise during sync runs:
  - `sync_prices` may emit Stacks unsupported-token DQ rows from broader seeded universe; this is not specific to Morpho/Euler/Dolomite adapter correctness.

## Open decision: Euler vault surface

Euler v2 still needs an explicit product decision on how to represent borrow surface discovery:

- Current implementation:
  - scans configured `vaults` and reads `balanceOf` + `debtOf` from each vault.
  - this works when supply/borrow are represented on the same vault surface.
- Decision needed before widening coverage:
  - whether config should remain supply-vault-centric, or
  - add explicit borrow-vault / paired-vault mapping in config for strategies where debt is opened through a separate vault surface.

Until this is decided, treat Euler coverage as correct for currently configured vaults, but potentially incomplete for strategies that separate collateral and debt vault addresses.

## Agent start prompt

```text
Deliver Sprint 04.

Read:
- docs/memory.md (yield expects supply/borrow/reward APY)
- docs/data-model.md (canonical snapshot fields)
- docs/testing.md (golden wallets + invariants)

Implement:
- Morpho Blue adapter (priority)
- Euler v2 adapter
- Dolomite adapter

Tests:
- golden wallet tests + invariants for each protocol

Avoid:
- changing SnapshotRunner interfaces (they should be frozen after Sprint 02)
- coupling these adapters together (each must stand alone)
```
