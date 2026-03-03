# agents.md

This file is intentionally short. It is a *directory* of context so agents can load only what they need.

## Core context (read for almost any task)

- `docs/memory.md` — business invariants (fees, yield definition, time boundaries, scope)
- `docs/architecture.md` — system design, module boundaries, data flow
- `docs/tech-stack.md` — languages, libraries, and repo conventions
- `docs/testing.md` — testing approach (golden wallets, VCR cassettes, invariants)
- `docs/data-model.md` — canonical schemas and units
- `docs/why.md` — rationale behind the critical decisions (to prevent “clever” regressions)

## Sprint plan

- `docs/prd/README.md` — sprint index + dependency order
- `docs/prd/sprint-XX-*.md` — each sprint has: scope, tests, DoD, and an agent-start prompt

## Prompts library (useful starting points)

- `docs/prompts/agent-template.md` — generic engineering prompt
- `docs/prompts/adapter-template.md` — protocol adapter prompt
- `docs/prompts/analytics-template.md` — yield/fees/ROE prompt
- `docs/prompts/dashboard-template.md` — API + UI prompt

## Repo conventions (important)

- Keep adapter code isolated under `src/adapters/<protocol>/...`
- Do not change canonical schemas without:
  1) updating `docs/data-model.md`,
  2) adding/adjusting migrations,
  3) updating all affected tests.

## If you are implementing a protocol adapter

Read, in order:
1) `docs/memory.md` (yield definition and what “position” means)
2) `docs/data-model.md` (fields + units)
3) `docs/testing.md` (golden wallet strategy)
4) the relevant sprint file under `docs/prd/`

Then use `docs/prompts/adapter-template.md` as the base prompt.
