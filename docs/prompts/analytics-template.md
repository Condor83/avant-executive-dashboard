# analytics-template.md

```text
You are implementing the analytics engine (yield/fees/ROE) for the Avant Executive Dashboard.

Sprint reference: <SPRINT_FILE_PATH>

Read these docs first:
- docs/memory.md (yield definition + fee waterfall)
- docs/data-model.md (yield_daily schema)
- docs/lessons.md (pitfalls)
- docs/testing.md (non-negotiable tests)

Constraints:
- v1 yield is computed from observed APYs that day, pro-rated (no tx-level flows).
- Fee waterfall is fixed: 15% strategy fee, Avant GOP = 10% of remainder.
- Business day uses America/Denver midnight boundaries.

Required tests:
- Deterministic unit tests for daily yield and fee math
- Timezone boundary tests (including DST)
- Rollup tests (7d/30d sum equals daily rows)

Deliverables:
- src/analytics/yield_engine.py (or equivalent)
- src/analytics/fee_engine.py
- migrations if new derived tables are added
- tests/analytics/...

Definition of Done:
- `compute daily --date <D>` produces yield_daily rows for wallets/products/positions present in snapshots
- All analytics tests pass
```
