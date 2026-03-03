"""Rollup consistency tests for daily and trailing-window analytics."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from analytics.fee_engine import apply_fee_waterfall
from analytics.rollups import compute_window_rollups
from analytics.yield_engine import (
    METHOD_APY_PRORATED_SOD_EOD,
    YieldEngine,
    denver_business_bounds_utc,
)
from core.db.models import (
    Chain,
    Market,
    PositionSnapshot,
    Product,
    Protocol,
    Wallet,
    WalletProductMap,
    YieldDaily,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _sum_metrics(rows: list[YieldDaily]) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    gross = Decimal("0")
    strategy_fee = Decimal("0")
    avant_gop = Decimal("0")
    net = Decimal("0")
    for row in rows:
        gross += row.gross_yield_usd
        strategy_fee += row.strategy_fee_usd
        avant_gop += row.avant_gop_usd
        net += row.net_yield_usd
    return gross, strategy_fee, avant_gop, net


def test_compute_daily_rollups_match_position_rows(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 3)
    sod_ts, eod_ts = denver_business_bounds_utc(business_date)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet_one = Wallet(
            address="0x1111111111111111111111111111111111111111", wallet_type="strategy"
        )
        wallet_two = Wallet(
            address="0x2222222222222222222222222222222222222222", wallet_type="strategy"
        )
        product_one = Product(product_code="stablecoin_senior")
        product_two = Product(product_code="stablecoin_junior")
        session.add_all([chain, protocol, wallet_one, wallet_two, product_one, product_two])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        session.add(market)
        session.flush()

        session.add_all(
            [
                WalletProductMap(wallet_id=wallet_one.wallet_id, product_id=product_one.product_id),
                WalletProductMap(wallet_id=wallet_two.wallet_id, product_id=product_two.product_id),
            ]
        )

        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=sod_ts,
                    wallet_id=wallet_one.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-1",
                    supplied_amount=Decimal("100"),
                    supplied_usd=Decimal("100"),
                    borrowed_amount=Decimal("20"),
                    borrowed_usd=Decimal("20"),
                    supply_apy=Decimal("0.10"),
                    borrow_apy=Decimal("0.04"),
                    reward_apy=Decimal("0.01"),
                    equity_usd=Decimal("80"),
                    source="rpc",
                    block_number_or_slot="1",
                ),
                PositionSnapshot(
                    as_of_ts_utc=eod_ts,
                    wallet_id=wallet_one.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-1",
                    supplied_amount=Decimal("110"),
                    supplied_usd=Decimal("110"),
                    borrowed_amount=Decimal("25"),
                    borrowed_usd=Decimal("25"),
                    supply_apy=Decimal("0.11"),
                    borrow_apy=Decimal("0.05"),
                    reward_apy=Decimal("0.015"),
                    equity_usd=Decimal("85"),
                    source="rpc",
                    block_number_or_slot="2",
                ),
                PositionSnapshot(
                    as_of_ts_utc=sod_ts,
                    wallet_id=wallet_two.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-2",
                    supplied_amount=Decimal("50"),
                    supplied_usd=Decimal("50"),
                    borrowed_amount=Decimal("5"),
                    borrowed_usd=Decimal("5"),
                    supply_apy=Decimal("0.08"),
                    borrow_apy=Decimal("0.03"),
                    reward_apy=Decimal("0.02"),
                    equity_usd=Decimal("45"),
                    source="rpc",
                    block_number_or_slot="1",
                ),
                PositionSnapshot(
                    as_of_ts_utc=eod_ts,
                    wallet_id=wallet_two.wallet_id,
                    market_id=market.market_id,
                    position_key="pos-2",
                    supplied_amount=Decimal("48"),
                    supplied_usd=Decimal("48"),
                    borrowed_amount=Decimal("4"),
                    borrowed_usd=Decimal("4"),
                    supply_apy=Decimal("0.075"),
                    borrow_apy=Decimal("0.028"),
                    reward_apy=Decimal("0.018"),
                    equity_usd=Decimal("44"),
                    source="rpc",
                    block_number_or_slot="2",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = YieldEngine(session).compute_daily(business_date=business_date)
        session.commit()

        assert summary.position_rows_written == 2
        assert summary.rollup_rows_written == 6

        rows = session.scalars(
            select(YieldDaily).where(YieldDaily.business_date == business_date)
        ).all()
        position_rows = [row for row in rows if row.position_key is not None]
        wallet_rows = [
            row
            for row in rows
            if row.position_key is None
            and row.wallet_id is not None
            and row.product_id is None
            and row.protocol_id is None
        ]
        product_rows = [
            row
            for row in rows
            if row.position_key is None
            and row.product_id is not None
            and row.wallet_id is None
            and row.protocol_id is None
        ]
        protocol_rows = [
            row
            for row in rows
            if row.position_key is None
            and row.protocol_id is not None
            and row.wallet_id is None
            and row.product_id is None
        ]
        total_row = next(
            row
            for row in rows
            if row.position_key is None
            and row.wallet_id is None
            and row.product_id is None
            and row.protocol_id is None
        )

        position_totals = _sum_metrics(position_rows)
        wallet_totals = _sum_metrics(wallet_rows)
        product_totals = _sum_metrics(product_rows)
        protocol_totals = _sum_metrics(protocol_rows)
        total_totals = (
            total_row.gross_yield_usd,
            total_row.strategy_fee_usd,
            total_row.avant_gop_usd,
            total_row.net_yield_usd,
        )

        assert position_totals == wallet_totals == product_totals == protocol_totals == total_totals


def test_window_rollups_equal_sum_of_daily_rows(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    first_date = date(2026, 1, 1)
    end_date = date(2026, 1, 30)

    with Session(engine) as session:
        protocol = Protocol(protocol_code="morpho")
        wallet_one = Wallet(
            address="0x3333333333333333333333333333333333333333", wallet_type="strategy"
        )
        wallet_two = Wallet(
            address="0x4444444444444444444444444444444444444444", wallet_type="strategy"
        )
        product_one = Product(product_code="btc_senior")
        product_two = Product(product_code="btc_junior")
        session.add_all([protocol, wallet_one, wallet_two, product_one, product_two])
        session.flush()

        for day_offset in range(30):
            business_date = first_date + timedelta(days=day_offset)
            gross_one = Decimal("10") + Decimal(day_offset)
            gross_two = Decimal("5") + (Decimal(day_offset) / Decimal("2"))
            fees_one = apply_fee_waterfall(gross_one)
            fees_two = apply_fee_waterfall(gross_two)

            session.add_all(
                [
                    YieldDaily(
                        business_date=business_date,
                        wallet_id=wallet_one.wallet_id,
                        product_id=product_one.product_id,
                        protocol_id=protocol.protocol_id,
                        market_id=None,
                        position_key=f"w1-{day_offset}",
                        gross_yield_usd=fees_one.gross_yield_usd,
                        strategy_fee_usd=fees_one.strategy_fee_usd,
                        avant_gop_usd=fees_one.avant_gop_usd,
                        net_yield_usd=fees_one.net_yield_usd,
                        method=METHOD_APY_PRORATED_SOD_EOD,
                        confidence_score=Decimal("1"),
                    ),
                    YieldDaily(
                        business_date=business_date,
                        wallet_id=wallet_two.wallet_id,
                        product_id=product_two.product_id,
                        protocol_id=protocol.protocol_id,
                        market_id=None,
                        position_key=f"w2-{day_offset}",
                        gross_yield_usd=fees_two.gross_yield_usd,
                        strategy_fee_usd=fees_two.strategy_fee_usd,
                        avant_gop_usd=fees_two.avant_gop_usd,
                        net_yield_usd=fees_two.net_yield_usd,
                        method=METHOD_APY_PRORATED_SOD_EOD,
                        confidence_score=Decimal("1"),
                    ),
                ]
            )
        session.commit()

    with Session(engine) as session:
        for window, days in (("7d", 7), ("30d", 30)):
            result = compute_window_rollups(session, window=window, end_date=end_date)
            assert result.start_date == end_date - timedelta(days=days - 1)

            totals = session.execute(
                select(
                    func.sum(YieldDaily.gross_yield_usd),
                    func.sum(YieldDaily.strategy_fee_usd),
                    func.sum(YieldDaily.avant_gop_usd),
                    func.sum(YieldDaily.net_yield_usd),
                ).where(
                    YieldDaily.method == METHOD_APY_PRORATED_SOD_EOD,
                    YieldDaily.position_key.is_not(None),
                    YieldDaily.business_date >= result.start_date,
                    YieldDaily.business_date <= result.end_date,
                )
            ).one()

            assert result.total.gross_yield_usd == totals[0]
            assert result.total.strategy_fee_usd == totals[1]
            assert result.total.avant_gop_usd == totals[2]
            assert result.total.net_yield_usd == totals[3]
