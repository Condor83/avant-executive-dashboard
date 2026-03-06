"""Runner persistence tests for market snapshot adapter outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from core.config import load_markets_config
from core.db.models import Chain, DataQuality, Market, MarketSnapshot, Protocol
from core.runner import SnapshotRunner
from core.types import DataQualityIssue, MarketSnapshotInput


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
                    max_ltv=Decimal("0.8"),
                    liquidation_threshold=Decimal("0.85"),
                    liquidation_penalty=Decimal("0.05"),
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


class ExplodingMarketAdapter:
    protocol_code = "wallet_balances"

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        del as_of_ts_utc
        del prices_by_token
        raise RuntimeError("market rpc timeout")


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

        utilization_total = session.scalar(select(func.sum(MarketSnapshot.utilization)))
        assert utilization_total == Decimal("0.5000000000")
        first_market = session.scalar(
            select(MarketSnapshot).where(
                MarketSnapshot.market_id
                == select(Market.market_id)
                .where(Market.market_address == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .scalar_subquery()
            )
        )
        assert first_market is not None
        assert first_market.max_ltv == Decimal("0.8000000000")
        assert first_market.liquidation_threshold == Decimal("0.8500000000")
        assert first_market.liquidation_penalty == Decimal("0.0500000000")


def test_runner_sync_markets_records_adapter_exception_and_keeps_healthy_rows(
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
            market_adapters=[MockMarketAdapter(), ExplodingMarketAdapter()],
        )
        summary = runner.sync_markets(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 2
        assert summary.issues_written == 1
        assert summary.component_failures == 1

        row_count = session.scalar(select(func.count()).select_from(MarketSnapshot))
        assert row_count == 2

        issue = session.scalars(select(DataQuality)).one()
        assert issue.stage == "sync_markets"
        assert issue.error_type == "market_adapter_exception"
        assert issue.protocol_code == "wallet_balances"
        assert issue.payload_json == {
            "exception_class": "RuntimeError",
            "detail": "market rpc timeout",
        }
