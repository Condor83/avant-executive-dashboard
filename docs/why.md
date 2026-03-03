# why.md (rationale)

This file explains *why* the repo is structured the way it is, so agents don’t “optimize away” critical guarantees.

## Why `markets.yaml` is config-driven (not discovery)

Discovery (e.g., DeBank) is great for coverage, but not a reliable source of truth for:
- exact market addresses
- protocol versions
- “which markets matter” for the strategy

`markets.yaml` makes ingestion deterministic and auditable.

## Why snapshot-first (append-only) storage

Executives care about correctness and reproducibility.
- Snapshots are replayable.
- Derived metrics can be rebuilt if formulas evolve.
- Debugging is possible without trusting external APIs retroactively.

## Why yield is computed from observed APY, not equity delta (v1)

- Strategies are delta neutral; yield is net interest.
- We avoid tx-level flow accounting in v1.
- APY-prorated yield is stable and explainable.

## Why adapters are isolated by protocol

Protocol math is brittle and unique (shares, indexes, IRMs).
Isolation prevents:
- cross-protocol coupling bugs
- merge conflicts during parallel development
- “one-size-fits-all” abstractions that leak

## Why “data quality” is a first-class output

A dashboard that *looks* precise but is missing markets is worse than no dashboard.
Coverage and freshness must be visible.

## Why `agents.md` is small

Agents should load only relevant context to avoid:
- token bloat
- stale assumptions
- accidental global refactors

Sprint files and prompts point agents to the minimal required context.
