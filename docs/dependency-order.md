# dependency-order.md (and parallelization plan)

## The critical freeze point

**After Sprint 02**, the following are considered “frozen interfaces” so work can parallelize safely:

1) Canonical schemas (`position_snapshots`, `market_snapshots`, `prices`, `yield_daily`)
2) Adapter interface and output types
3) Config contract (fields in `markets.yaml` and any new `consumer_markets.yaml`)

Adapters and downstream analytics should not require schema churn after this point.

## Sprint dependency order

- Sprint 00 → Sprint 01 → Sprint 02 are strictly sequential.
- After Sprint 02, the following can proceed in parallel:

### Parallel lane A — Protocol adapters (separate worktrees recommended)
- Aave v3 adapter
- Morpho Blue adapter (+ MetaMorpho vault handling if needed)
- Euler v2 adapter
- Dolomite adapter
- Silo v2 adapter (consumer markets)
- Kamino adapter (Solana)
- Zest adapter (Stacks)
- Wallet balances adapter (if not completed)

### Parallel lane B — Analytics
- Yield engine + fee engine
- Rollups (yesterday/7d/30d)
- Risk scoring (kink + spread compression)

### Parallel lane C — API + Dashboard
- FastAPI routes and query layer
- UI scaffolding with mocked responses
- Drilldown UX + design system

## How to avoid dirty worktrees

Recommended approach:
- Use `git worktree` per lane (or per adapter).
- Each adapter lane only touches:
  - `src/adapters/<protocol>/`
  - `tests/adapters/<protocol>/`
  - adapter docs (if any)
- Changes to shared interfaces require a dedicated PR and must land before adapter work merges.

## Merge discipline

- Keep PRs small and single-purpose.
- Prefer adding new modules rather than editing existing ones across many files.
- Never merge an adapter without:
  - golden wallet tests
  - at least one end-to-end ingestion run that produces snapshots
