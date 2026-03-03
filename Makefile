.PHONY: fmt lint test typecheck check

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
