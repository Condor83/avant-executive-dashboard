# tech-stack.md

This is the recommended stack for the MVP + production hardening.

## Backend

- **Python** (3.11+)
- Packaging: `poetry` or `uv` (pick one; keep CI consistent)
- HTTP: `httpx` (async-friendly)
- Data validation: `pydantic`
- Database: **Postgres (Neon)** + `SQLAlchemy` + `Alembic`
- Testing: `pytest`, `pytest-asyncio`, `vcrpy` (record external HTTP), `responses`
- Lint/format: `ruff`, `black`, `mypy` (optional but recommended)
- CLI: `typer` (clean commands) or `click`

## Chain clients

- EVM: `web3.py` (plus multicall batching); optional `eth_retry`
- Solana: `solders` + `solana-py` (or RPC via `httpx`)
- Stacks: `httpx` + Hiro/Stakes node APIs (read-only calls)

## Data sources

- RPC provider: Alchemy (EVM + Solana)
- Indexed discovery: DeBank Cloud
- Prices/yields fallback: DefiLlama

## Orchestration

- MVP: manual CLI sync (`sync snapshot`, `compute daily`)
- Production: Prefect/Dagster/Airflow (choose once), plus cron for market snapshots

## Frontend

Two viable options:

1) **Next.js** (recommended for exec-grade polish)  
   - React, Tailwind, shadcn/ui
   - Server-side rendering for fast loads
2) **Metabase/Superset** (fast internal dashboards)  
   - Less custom polish, but very fast iteration

## Observability

- Structured logging (JSON logs)
- Metrics (Prometheus/OpenTelemetry)
- Alerts (Slack webhook)
