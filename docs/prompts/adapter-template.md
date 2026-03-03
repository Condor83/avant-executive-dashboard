# adapter-template.md

```text
You are implementing a protocol adapter in the Avant Executive Dashboard repo.

Task:
Implement adapter: <PROTOCOL_NAME>
Sprint reference: <SPRINT_FILE_PATH>

Read these docs first:
- docs/memory.md
- docs/data-model.md
- docs/testing.md
- docs/why.md (adapter isolation rationale)

Constraints:
- Adapter must read protocol/chain/market config from markets.yaml (or consumer_markets.yaml for consumer-only markets).
- Output must conform exactly to PositionSnapshot + MarketSnapshot canonical fields.
- All amounts must be normalized using correct decimals.
- Store normalized APYs in 0.0–1.0 units (e.g., 0.05 for 5%).

Required tests:
- Golden wallet integration tests (use config wallets as fixtures)
- Invariant tests: non-negative balances, equity identity, utilization bounds

Deliverables:
- src/adapters/<protocol>/...
- tests/adapters/<protocol>/...
- Any needed config parsing helpers (do not modify global schemas unless required)

Definition of Done:
- Adapter produces snapshots for at least one chain in markets.yaml
- Tests pass in CI
- Snapshot runner can ingest without errors for the adapter’s configured markets
```
