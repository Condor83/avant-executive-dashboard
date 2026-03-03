# dashboard-template.md

```text
You are implementing the API + Dashboard layer for the Avant Executive Dashboard.

Sprint reference: <SPRINT_FILE_PATH>

Read these docs first:
- docs/memory.md (what executives need to see)
- docs/architecture.md (data flow)
- docs/data-model.md (canonical schemas)
- docs/why.md (auditability + data quality)

Constraints:
- Executive UI must be “sharp”: minimal clicks, clear KPIs, fast load.
- All displayed numbers must be traceable to stored snapshots / derived tables.
- Include a visible “data quality” indicator (freshness + coverage).

Required tests:
- API schema tests (response shape, required fields)
- Basic query correctness tests (e.g., totals match sum of components)

Deliverables:
- src/api/... (FastAPI)
- frontend/... (if chosen)
- docs describing how to run locally

Definition of Done:
- Summary page shows yesterday/7d/30d yield, Avant GOP, ROE after fees
- Drilldowns by product → wallet → position are functional
```
