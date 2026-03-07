"""Runner persistence tests with mocked adapter outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from adapters.pendle import PendleTrade
from core.config import PTFixedYieldOverride, load_markets_config
from core.db.models import (
    Chain,
    DataQuality,
    Market,
    MarketSnapshot,
    PositionFixedYieldCache,
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


class MockPendlePtAdapter:
    protocol_code = "morpho"

    def __init__(self, *, collateral_amount: Decimal) -> None:
        self.collateral_amount = collateral_amount

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
                    protocol_code="morpho",
                    chain_code="ethereum",
                    wallet_address="0x1491b385d4f80c524540b05a080179e5550ab0f9",
                    market_ref="0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076",
                    position_key=(
                        "morpho:ethereum:0x1491b385d4f80c524540b05a080179e5550ab0f9:"
                        "0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076"
                    ),
                    supplied_amount=Decimal("0"),
                    supplied_usd=Decimal("0"),
                    borrowed_amount=Decimal("314166.371938956756814463"),
                    borrowed_usd=Decimal("314166.371938956756814463"),
                    supply_apy=Decimal("0.0614795731"),
                    borrow_apy=Decimal("0.032"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("265297.198174026311759477"),
                    collateral_amount=self.collateral_amount,
                    collateral_usd=Decimal("579463.570112983068573940"),
                    source="rpc",
                )
            ],
            [],
        )


class StubPendleHistoryClient:
    def __init__(
        self, *, trades: list[object] | None = None, error: Exception | None = None
    ) -> None:
        self.trades = trades or []
        self.error = error
        self.trade_calls = 0
        self.market_calls = 0

    def close(self) -> None:
        return None

    def get_market_addresses_for_pt(self, *, chain_id: int, pt_token_address: str) -> set[str]:
        del chain_id, pt_token_address
        self.market_calls += 1
        if self.error is not None:
            raise self.error
        return {"0xpendlemarket"}

    def get_wallet_trades(self, *, chain_id: int, wallet_address: str):
        del chain_id, wallet_address
        self.trade_calls += 1
        if self.error is not None:
            raise self.error
        return list(self.trades)


def _seed_morpho_pt_dimensions(session: Session) -> None:
    chain = Chain(chain_code="ethereum")
    protocol = Protocol(protocol_code="morpho")
    wallet = Wallet(
        address="0x1491b385d4f80c524540b05a080179e5550ab0f9",
        wallet_type="strategy",
    )
    session.add_all([chain, protocol, wallet])
    session.flush()

    loan_token = Token(
        chain_id=chain.chain_id,
        address_or_mint="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        symbol="USDC",
        decimals=6,
    )
    pt_token = Token(
        chain_id=chain.chain_id,
        address_or_mint="0x606b5c773dc4d6e625c411cf60565f8c30c467d2",
        symbol="PT-savUSD-14MAY2026",
        decimals=18,
    )
    session.add_all([loan_token, pt_token])
    session.flush()

    session.add(
        Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076",
            market_kind="market",
            display_name="PT-savUSD-14MAY2026 / USDC",
            base_asset_token_id=loan_token.token_id,
            collateral_token_id=pt_token.token_id,
            metadata_json={"kind": "market"},
        )
    )
    session.commit()


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


def test_runner_sync_snapshot_caches_pt_fixed_apy_and_reuses_it_when_size_is_stable(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        _seed_morpho_pt_dimensions(session)

    first_pendle = StubPendleHistoryClient(
        trades=[
            PendleTrade(
                market_address="0xpendlemarket",
                action="BUY_PT",
                timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.05"),
                pt_notional=Decimal("300"),
            ),
            PendleTrade(
                market_address="0xpendlemarket",
                action="BUY_PT",
                timestamp=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.09"),
                pt_notional=Decimal("300"),
            ),
            PendleTrade(
                market_address="0xpendlemarket",
                action="SELL_PT",
                timestamp=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.07"),
                pt_notional=Decimal("100"),
            ),
        ]
    )

    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("500"))],
            pendle_history_client=first_pendle,
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 0
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0.0740000000")

        cache_row = session.scalar(select(PositionFixedYieldCache))
        assert cache_row is not None
        assert cache_row.fixed_apy == Decimal("0.0740000000")
        assert cache_row.position_size_native_at_refresh == Decimal("500")
        assert cache_row.lot_count == 2
        assert first_pendle.market_calls == 1
        assert first_pendle.trade_calls == 1

    second_pendle = StubPendleHistoryClient()
    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("500.5"))],
            pendle_history_client=second_pendle,
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 0
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0.0740000000")
        assert second_pendle.market_calls == 0
        assert second_pendle.trade_calls == 0


def test_runner_sync_snapshot_refreshes_pt_fixed_apy_when_balance_grows(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        _seed_morpho_pt_dimensions(session)
        session.add(
            PositionFixedYieldCache(
                position_key=(
                    "morpho:ethereum:0x1491b385d4f80c524540b05a080179e5550ab0f9:"
                    "0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076"
                ),
                protocol_code="morpho",
                chain_code="ethereum",
                wallet_address="0x1491b385d4f80c524540b05a080179e5550ab0f9",
                market_ref="0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076",
                collateral_symbol="PT-savUSD-14MAY2026",
                fixed_apy=Decimal("0.074"),
                source="pendle_history",
                position_size_native_at_refresh=Decimal("500"),
                position_size_usd_at_refresh=Decimal("579463.570112983068573940"),
                lot_count=2,
                first_acquired_at_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                last_refreshed_at_utc=as_of_ts,
                metadata_json={},
            )
        )
        session.commit()

    pendle = StubPendleHistoryClient(
        trades=[
            PendleTrade(
                market_address="0xpendlemarket",
                action="BUY_PT",
                timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.05"),
                pt_notional=Decimal("300"),
            ),
            PendleTrade(
                market_address="0xpendlemarket",
                action="BUY_PT",
                timestamp=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.09"),
                pt_notional=Decimal("300"),
            ),
            PendleTrade(
                market_address="0xpendlemarket",
                action="SELL_PT",
                timestamp=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.07"),
                pt_notional=Decimal("100"),
            ),
            PendleTrade(
                market_address="0xpendlemarket",
                action="BUY_PT",
                timestamp=datetime(2026, 3, 4, 0, 0, tzinfo=UTC),
                implied_apy=Decimal("0.10"),
                pt_notional=Decimal("20"),
            ),
        ]
    )

    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("520"))],
            pendle_history_client=pendle,
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 0
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0.0750000000")

        cache_row = session.scalar(select(PositionFixedYieldCache))
        assert cache_row is not None
        assert cache_row.fixed_apy == Decimal("0.0750000000")
        assert cache_row.position_size_native_at_refresh == Decimal("520")
        assert pendle.market_calls == 1
        assert pendle.trade_calls == 1


def test_runner_sync_snapshot_zeroes_pt_supply_apy_when_pendle_history_is_unresolved(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        _seed_morpho_pt_dimensions(session)

    pendle = StubPendleHistoryClient(trades=[])
    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("500"))],
            pendle_history_client=pendle,
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 1
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0")
        issue = session.scalars(select(DataQuality)).one()
        assert issue.error_type == "pt_fixed_apy_unresolved"


def test_runner_sync_snapshot_uses_manual_pt_fixed_yield_override_before_pendle_history(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        _seed_morpho_pt_dimensions(session)

    pendle = StubPendleHistoryClient(trades=[])
    override = PTFixedYieldOverride(
        position_key=(
            "morpho:ethereum:0x1491b385d4f80c524540b05a080179e5550ab0f9:"
            "0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076"
        ),
        fixed_apy=Decimal("0.6256258359"),
        source="etherscan_manual",
        tx_hash="0x1669e80269280df433959c54e04c49dc66d1cc507665f9aeb92a713b81d101ca",
        acquired_at_utc="2026-01-31T00:51:35Z",
        note="manual stopgap",
    )
    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("500"))],
            pendle_history_client=pendle,
            pt_fixed_yield_overrides={override.position_key: override},
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 0
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0.6256258359")
        assert pendle.market_calls == 0
        assert pendle.trade_calls == 0
        assert session.scalar(select(func.count()).select_from(DataQuality)) == 0


def test_runner_sync_snapshot_uses_manual_pt_fixed_yield_override_without_pendle_client(
    postgres_database_url: str,
) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    as_of_ts = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        _seed_morpho_pt_dimensions(session)

    override = PTFixedYieldOverride(
        position_key=(
            "morpho:ethereum:0x1491b385d4f80c524540b05a080179e5550ab0f9:"
            "0xc978f01522ff64adafd91856065d602c56e326a0368b895bd9244d5998e60076"
        ),
        fixed_apy=Decimal("0.6256258359"),
        source="etherscan_manual",
        tx_hash="0x1669e80269280df433959c54e04c49dc66d1cc507665f9aeb92a713b81d101ca",
        acquired_at_utc="2026-01-31T00:51:35Z",
        note="manual stopgap",
    )
    with Session(engine) as session:
        runner = SnapshotRunner(
            session=session,
            markets_config=load_markets_config("config/markets.yaml"),
            price_oracle=None,
            position_adapters=[MockPendlePtAdapter(collateral_amount=Decimal("500"))],
            pendle_history_client=None,
            pt_fixed_yield_overrides={override.position_key: override},
        )
        summary = runner.sync_snapshot(as_of_ts_utc=as_of_ts)
        session.commit()

        assert summary.rows_written == 1
        assert summary.issues_written == 0
        snapshot = session.scalar(select(PositionSnapshot))
        assert snapshot is not None
        assert snapshot.supply_apy == Decimal("0.6256258359")
        assert session.scalar(select(func.count()).select_from(DataQuality)) == 0
