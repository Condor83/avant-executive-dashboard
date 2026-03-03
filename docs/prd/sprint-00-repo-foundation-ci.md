# Sprint 00 — Repo foundation & CI


## Outcome

A working repository skeleton with CI and local dev tooling so the team can build in parallel without constantly fixing environments.

## Scope

### In
- Python project scaffolding
- Formatting/linting/typecheck baseline
- Test harness + CI pipeline
- Local Postgres via docker compose
- Minimal CLI skeleton

### Out
- Any protocol adapter logic
- Any production deployment

## Deliverables

- Standard repo layout:
  - `src/core`, `src/adapters`, `src/analytics`, `src/api`
  - `tests/` mirroring structure
  - `docs/` directory (this PRD pack can be committed as-is)
- Tooling:
  - `Makefile` (or `justfile`) with `make lint`, `make test`, `make fmt`
  - `pre-commit` hooks
- CI:
  - runs lint + unit tests on every PR
- Local dev:
  - `docker-compose.yml` with Postgres
  - `.env.example`

## Tests (must be written and passing)

- `tests/test_smoke.py`
  - imports package modules
  - asserts CLI entrypoint runs `--help` without error
- `tests/core/test_settings.py`
  - validates env var parsing and defaults

## Done looks like

- [ ] `make test` passes locally
- [ ] CI passes on a clean PR
- [ ] `docker compose up` brings up Postgres and a healthcheck passes
- [ ] No secrets in repo; `.env.example` present

## Agent start prompt

```text
Deliver Sprint 00.

Read:
- docs/agents.md (core context list)
- docs/tech-stack.md (tooling preferences)

Build:
- repo skeleton + CI + local Postgres compose + CLI stub

Tests:
- add the Sprint 00 smoke tests and ensure CI runs them

Do not:
- start implementing adapters or analytics
- introduce framework complexity beyond what Sprint 00 needs
```
