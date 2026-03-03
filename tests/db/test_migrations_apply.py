"""Migration application tests."""

from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def test_migrations_apply_cleanly(postgres_database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)

    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    inspector = inspect(engine)
    expected_tables = {
        "wallets",
        "products",
        "wallet_product_map",
        "protocols",
        "chains",
        "tokens",
        "markets",
        "position_snapshots",
        "market_snapshots",
        "prices",
        "data_quality",
        "yield_daily",
    }
    assert expected_tables.issubset(set(inspector.get_table_names()))
