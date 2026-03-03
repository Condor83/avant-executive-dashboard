"""Runner persistence tests for market snapshot adapter outputs."""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from core.config import load_markets_config
from core.db.models import Chain, Market, MarketSnapshot, Protocol
from core.runner import SnapshotRunner
from core.types import DataQualityIssue, MarketSnapshotInput

DEFAULT_TEST_ADMIN_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"


@pytest.fixture()
def postgres_database_url() -> Generator[str, None, None]:
    admin_url = make_url(os.getenv("AVANT_TEST_DATABASE_URL", DEFAULT_TEST_ADMIN_URL))
    db_name = f"avant_runner_market_test_{uuid4().hex[:12]}"

    admin_psycopg_dsn = admin_url.render_as_string(hide_password=False).replace("+psycopg", "")
    test_url = admin_url.set(database=db_name).render_as_string(hide_password=False)

    try:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable for runner market tests: {exc}")

    try:
        yield test_url
    finally:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))


class MockMarketAdapter:
    protocol_code = "wallet_balances"

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        del prices_by_token
        return (
            [
                MarketSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code="wallet_balances",
                    chain_code="ethereum",
                    market_ref="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    total_supply_usd=Decimal("100"),
                    total_borrow_usd=Decimal("40"),
                    utilization=Decimal("0.4"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    source="rpc",
                ),
                MarketSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code="wallet_balances",
                    chain_code="ethereum",
                    market_ref="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    total_supply_usd=Decimal("50"),
                    total_borrow_usd=Decimal("5"),
                    utilization=Decimal("0.1"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    source="rpc",
                ),
            ],
            [],
        )


def test_runner_writes_market_rows(postgres_database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="wallet_balances")
        session.add(chain)
        session.add(protocol)
        session.flush()

        session.add_all(
            [
                Market(
                    chain_id=chain.chain_id,
                    protocol_id=protocol.protocol_id,
                    market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    base_asset_token_id=None,
                    collateral_token_id=None,
                    metadata_json={"kind": "wallet_balance_token"},
                ),
                Market(
                    chain_id=chain.chain_id,
                    protocol_id=protocol.protocol_id,
                    market_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    base_asset_token_id=None,
                    collateral_token_id=None,
                    metadata_json={"kind": "wallet_balance_token"},
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[],
            market_adapters=[MockMarketAdapter()],
        )
        summary = runner.sync_markets(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 2
        assert summary.issues_written == 0

        row_count = session.scalar(select(func.count()).select_from(MarketSnapshot))
        assert row_count == 2

        utilization_total = session.scalar(select(func.sum(MarketSnapshot.utilization)))
        assert utilization_total == Decimal("0.5000000000")
