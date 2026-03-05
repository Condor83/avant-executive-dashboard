"""Market overview analytics computation tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.market_engine import MarketEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Chain,
    Market,
    MarketOverviewDaily,
    MarketSnapshot,
    PositionSnapshot,
    Protocol,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_market_overview_builds_deterministic_rows(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 3)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=12)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x1111111111111111111111111111111111111111",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet])
        session.flush()

        market_1 = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        market_2 = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        session.add_all([market_1, market_2])
        session.flush()

        session.add_all(
            [
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=market_1.market_id,
                    total_supply_usd=Decimal("100"),
                    total_borrow_usd=Decimal("40"),
                    utilization=Decimal("0.4"),
                    supply_apy=Decimal("0.05"),
                    borrow_apy=Decimal("0.02"),
                    available_liquidity_usd=None,
                    max_ltv=Decimal("0.8"),
                    liquidation_threshold=Decimal("0.85"),
                    liquidation_penalty=Decimal("0.05"),
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                # Lower-priority duplicate source for same market and timestamp.
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=market_1.market_id,
                    total_supply_usd=Decimal("999"),
                    total_borrow_usd=Decimal("111"),
                    utilization=Decimal("0.1111111111"),
                    supply_apy=Decimal("0.90"),
                    borrow_apy=Decimal("0.01"),
                    available_liquidity_usd=Decimal("888"),
                    max_ltv=Decimal("0.1"),
                    liquidation_threshold=Decimal("0.2"),
                    liquidation_penalty=Decimal("0.3"),
                    caps_json=None,
                    irm_params_json=None,
                    source="defillama",
                ),
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=market_2.market_id,
                    total_supply_usd=Decimal("0"),
                    total_borrow_usd=Decimal("0"),
                    utilization=Decimal("0"),
                    supply_apy=Decimal("0.01"),
                    borrow_apy=Decimal("0.03"),
                    available_liquidity_usd=Decimal("0"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
            ]
        )

        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=market_1.market_id,
                    position_key="pos-1",
                    supplied_amount=Decimal("25"),
                    supplied_usd=Decimal("25"),
                    borrowed_amount=Decimal("10"),
                    borrowed_usd=Decimal("10"),
                    supply_apy=Decimal("0.05"),
                    borrow_apy=Decimal("0.02"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("15"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=market_2.market_id,
                    position_key="pos-2",
                    supplied_amount=Decimal("5"),
                    supplied_usd=Decimal("5"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.01"),
                    borrow_apy=Decimal("0.03"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("5"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = MarketEngine(session).compute_daily(business_date=business_date)
        session.commit()

        assert summary.rows_written == 2
        assert summary.as_of_ts_utc == as_of_ts_utc

        rows = list(
            session.scalars(
                select(MarketOverviewDaily)
                .where(MarketOverviewDaily.business_date == business_date)
                .order_by(MarketOverviewDaily.market_id.asc())
            )
        )
        assert len(rows) == 2

        row_1 = rows[0]
        assert row_1.source == "rpc"
        assert row_1.total_supply_usd == Decimal("100")
        assert row_1.total_borrow_usd == Decimal("40")
        assert row_1.available_liquidity_usd == Decimal("60")
        assert row_1.spread_apy == Decimal("0.03")
        assert row_1.avant_supplied_usd == Decimal("25")
        assert row_1.avant_borrowed_usd == Decimal("10")
        assert row_1.avant_supply_share == Decimal("0.25")
        assert row_1.avant_borrow_share == Decimal("0.25")
        assert row_1.max_ltv == Decimal("0.8")
        assert row_1.liquidation_threshold == Decimal("0.85")
        assert row_1.liquidation_penalty == Decimal("0.05")

        row_2 = rows[1]
        assert row_2.total_supply_usd == Decimal("0")
        assert row_2.total_borrow_usd == Decimal("0")
        assert row_2.spread_apy == Decimal("-0.02")
        assert row_2.avant_supply_share is None
        assert row_2.avant_borrow_share is None
