.PHONY: fmt lint test typecheck check db-up db-down db-migrate db-seed db-ui

fmt:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy src tests

test:
	uv run pytest

typecheck:
	uv run mypy src tests

check: lint test

db-up:
	docker compose up -d postgres adminer

db-down:
	docker compose down

db-migrate:
	uv run alembic upgrade head

db-seed:
	uv run python -m core.seed_db --markets config/markets.yaml --wallet-products config/wallet_products.yaml --consumer-markets config/consumer_markets.yaml

db-ui:
	@echo "Adminer is available at http://localhost:8080"
