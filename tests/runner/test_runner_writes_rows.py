"""Runner persistence tests with mocked adapter outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from core.config import load_markets_config
from core.db.models import (
    Chain,
    DataQuality,
    Market,
    MarketSnapshot,
    PositionSnapshot,
    Protocol,
    Token,
    Wallet,
)
from core.pricing import PriceOracle
from core.runner import SnapshotRunner
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput


class MockAdapter:
    protocol_code = "wallet_balances"

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        del prices_by_token
        return (
            [
                PositionSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code="wallet_balances",
                    chain_code="ethereum",
                    wallet_address="0x1111111111111111111111111111111111111111",
                    market_ref="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    position_key="wallet_balances:ethereum:0x1111111111111111111111111111111111111111:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    supplied_amount=Decimal("10"),
                    supplied_usd=Decimal("10"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("10"),
                    source="rpc",
                ),
                PositionSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code="wallet_balances",
                    chain_code="ethereum",
                    wallet_address="0x2222222222222222222222222222222222222222",
                    market_ref="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    position_key="wallet_balances:ethereum:0x2222222222222222222222222222222222222222:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    supplied_amount=Decimal("7.5"),
                    supplied_usd=Decimal("7.5"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("7.5"),
                    source="rpc",
                ),
            ],
            [],
        )


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
                    total_supply_usd=Decimal("10"),
                    total_borrow_usd=Decimal("2"),
                    utilization=Decimal("0.2"),
                    supply_apy=Decimal("0.03"),
                    borrow_apy=Decimal("0.05"),
                    available_liquidity_usd=Decimal("8"),
                    source="rpc",
                ),
                MarketSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code="wallet_balances",
                    chain_code="ethereum",
                    market_ref="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    total_supply_usd=Decimal("7.5"),
                    total_borrow_usd=Decimal("1.5"),
                    utilization=Decimal("0.2"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0.04"),
                    available_liquidity_usd=Decimal("6"),
                    source="rpc",
                ),
            ],
            [],
        )


class ExplodingPositionAdapter:
    protocol_code = "wallet_balances"

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        del as_of_ts_utc
        del prices_by_token
        raise RuntimeError("rpc timeout")


class ExplodingPriceOracle:
    def fetch_prices(
        self,
        requests,
        *,
        as_of_ts_utc: datetime,
    ):
        del requests
        del as_of_ts_utc
        raise RuntimeError("price service unavailable")


def test_runner_writes_position_rows(postgres_database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="wallet_balances")
        wallets = [
            Wallet(address="0x1111111111111111111111111111111111111111", wallet_type="strategy"),
            Wallet(address="0x2222222222222222222222222222222222222222", wallet_type="strategy"),
        ]
        session.add(chain)
        session.add(protocol)
        session.add_all(wallets)
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
            position_adapters=[MockAdapter()],
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 2
        assert summary.issues_written == 0
        assert summary.component_failures == 0

        row_count = session.scalar(select(func.count()).select_from(PositionSnapshot))
        assert row_count == 2

        usd_total = session.scalar(select(func.sum(PositionSnapshot.supplied_usd)))
        assert usd_total == Decimal("17.500000000000000000")


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
        assert summary.component_failures == 0

        row_count = session.scalar(select(func.count()).select_from(MarketSnapshot))
        assert row_count == 2

        utilization_avg = session.scalar(select(func.avg(MarketSnapshot.utilization)))
        assert utilization_avg == Decimal("0.2000000000")


def test_runner_sync_snapshot_records_adapter_exception_and_keeps_healthy_rows(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="wallet_balances")
        wallets = [
            Wallet(address="0x1111111111111111111111111111111111111111", wallet_type="strategy"),
            Wallet(address="0x2222222222222222222222222222222222222222", wallet_type="strategy"),
        ]
        session.add(chain)
        session.add(protocol)
        session.add_all(wallets)
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
            position_adapters=[MockAdapter(), ExplodingPositionAdapter()],
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 2
        assert summary.issues_written == 1
        assert summary.component_failures == 1

        row_count = session.scalar(select(func.count()).select_from(PositionSnapshot))
        assert row_count == 2

        issue = session.scalars(select(DataQuality)).one()
        assert issue.stage == "sync_snapshot"
        assert issue.error_type == "position_adapter_exception"
        assert issue.protocol_code == "wallet_balances"
        assert issue.payload_json == {
            "exception_class": "RuntimeError",
            "detail": "rpc timeout",
        }


def test_runner_sync_prices_records_price_oracle_exception(postgres_database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=cast(PriceOracle, ExplodingPriceOracle()),
            position_adapters=[],
        )
        summary = runner.sync_prices(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 0
        assert summary.issues_written == 1
        assert summary.component_failures == 1

        issue = session.scalars(select(DataQuality)).one()
        assert issue.stage == "sync_prices"
        assert issue.error_type == "price_oracle_exception"
        assert issue.payload_json == {
            "exception_class": "RuntimeError",
            "detail": "price service unavailable",
        }


def test_runner_sync_snapshot_records_price_oracle_exception_and_keeps_healthy_rows(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="wallet_balances")
        wallets = [
            Wallet(address="0x1111111111111111111111111111111111111111", wallet_type="strategy"),
            Wallet(address="0x2222222222222222222222222222222222222222", wallet_type="strategy"),
        ]
        session.add_all([chain, protocol, *wallets])
        session.flush()
        session.add(
            Token(
                chain_id=chain.chain_id,
                address_or_mint="0xcccccccccccccccccccccccccccccccccccccccc",
                symbol="USDC",
                decimals=6,
            )
        )
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
            price_oracle=cast(PriceOracle, ExplodingPriceOracle()),
            position_adapters=[MockAdapter()],
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 2
        assert summary.issues_written == 1
        assert summary.component_failures == 1

        row_count = session.scalar(select(func.count()).select_from(PositionSnapshot))
        assert row_count == 2

        issue = session.scalars(select(DataQuality)).one()
        assert issue.stage == "sync_snapshot"
        assert issue.error_type == "price_oracle_exception"
        assert issue.payload_json == {
            "exception_class": "RuntimeError",
            "detail": "price service unavailable",
        }
