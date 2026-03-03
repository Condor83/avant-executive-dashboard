# llm-routing.md (optional)

This is a pragmatic guide for selecting “how smart” the coding agent needs to be by task.

## Use higher reasoning (high/xhigh) for
- protocol adapter math (shares/indexes, fixed-point rate units)
- yield/fee/ROE computations
- anything that can silently misstate money

## Use medium reasoning for
- DB schema + migrations
- ingestion runner scaffolding
- query layer / API endpoints

## Use fast/light models (Spark) for
- UI polish and component iteration
- refactors that are well-covered by tests
- documentation updates

## Guardrails
- Require golden wallet tests for every adapter.
- Require deterministic unit tests for any yield/fee math changes.
- Prefer “replayable” inputs (block/slot pinned).
