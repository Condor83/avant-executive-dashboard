"""Market concentration metric tests."""

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


def test_concentration_uses_sum_of_positions_per_market(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 3)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)
    market_id = 0

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="morpho")
        wallet_a = Wallet(
            address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            wallet_type="strategy",
        )
        wallet_b = Wallet(
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet_a, wallet_b])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xcccccccccccccccccccccccccccccccccccccccc",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "market"},
        )
        session.add(market)
        session.flush()
        market_id = market.market_id

        session.add(
            MarketSnapshot(
                as_of_ts_utc=as_of_ts_utc,
                block_number_or_slot="1",
                market_id=market.market_id,
                total_supply_usd=Decimal("200"),
                total_borrow_usd=Decimal("100"),
                utilization=Decimal("0.5"),
                supply_apy=Decimal("0.04"),
                borrow_apy=Decimal("0.06"),
                available_liquidity_usd=Decimal("100"),
                max_ltv=Decimal("0.9"),
                liquidation_threshold=None,
                liquidation_penalty=None,
                caps_json=None,
                irm_params_json=None,
                source="rpc",
            )
        )

        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet_a.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-a",
                    supplied_amount=Decimal("20"),
                    supplied_usd=Decimal("20"),
                    borrowed_amount=Decimal("5"),
                    borrowed_usd=Decimal("5"),
                    supply_apy=Decimal("0.04"),
                    borrow_apy=Decimal("0.06"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("15"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet_b.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-b",
                    supplied_amount=Decimal("30"),
                    supplied_usd=Decimal("30"),
                    borrowed_amount=Decimal("10"),
                    borrowed_usd=Decimal("10"),
                    supply_apy=Decimal("0.04"),
                    borrow_apy=Decimal("0.06"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("20"),
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

        assert summary.rows_written == 1

        row = session.scalar(
            select(MarketOverviewDaily).where(
                MarketOverviewDaily.business_date == business_date,
                MarketOverviewDaily.market_id == market_id,
            )
        )
        assert row is not None
        assert row.avant_supplied_usd == Decimal("50")
        assert row.avant_borrowed_usd == Decimal("15")
        assert row.avant_supply_share == Decimal("0.25")
        assert row.avant_borrow_share == Decimal("0.15")
