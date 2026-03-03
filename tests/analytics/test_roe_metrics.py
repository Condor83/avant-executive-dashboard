"""ROE metric tests for daily and window rollup analytics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from analytics.rollups import compute_window_rollups
from analytics.yield_engine import METHOD_APY_PRORATED_SOD_EOD, compute_roe_breakdown
from core.db.models import YieldDaily


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_compute_roe_breakdown_positive_equity() -> None:
    roe = compute_roe_breakdown(
        gross_yield_usd=Decimal("100"),
        strategy_fee_usd=Decimal("15"),
        net_yield_usd=Decimal("76.5"),
        avant_gop_usd=Decimal("8.5"),
        avg_equity_usd=Decimal("1000"),
    )

    assert roe.gross_roe == Decimal("0.1")
    assert roe.post_strategy_fee_roe == Decimal("0.085")
    assert roe.net_roe == Decimal("0.0765")
    assert roe.avant_gop_roe == Decimal("0.0085")


def test_compute_roe_breakdown_non_positive_equity_returns_null_roes() -> None:
    roe = compute_roe_breakdown(
        gross_yield_usd=Decimal("5"),
        strategy_fee_usd=Decimal("0.75"),
        net_yield_usd=Decimal("3.825"),
        avant_gop_usd=Decimal("0.425"),
        avg_equity_usd=Decimal("0"),
    )

    assert roe.gross_roe is None
    assert roe.post_strategy_fee_roe is None
    assert roe.net_roe is None
    assert roe.avant_gop_roe is None


def test_window_rollups_compute_roe_as_ratio_of_sums(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    with Session(engine) as session:
        session.add_all(
            [
                YieldDaily(
                    business_date=date(2026, 3, 2),
                    wallet_id=1,
                    product_id=1,
                    protocol_id=1,
                    market_id=1,
                    position_key="p-1",
                    gross_yield_usd=Decimal("20"),
                    strategy_fee_usd=Decimal("3"),
                    avant_gop_usd=Decimal("1.7"),
                    net_yield_usd=Decimal("15.3"),
                    avg_equity_usd=Decimal("200"),
                    gross_roe=Decimal("0.1"),
                    post_strategy_fee_roe=Decimal("0.085"),
                    net_roe=Decimal("0.0765"),
                    avant_gop_roe=Decimal("0.0085"),
                    method=METHOD_APY_PRORATED_SOD_EOD,
                    confidence_score=Decimal("1"),
                ),
                YieldDaily(
                    business_date=date(2026, 3, 3),
                    wallet_id=2,
                    product_id=2,
                    protocol_id=2,
                    market_id=2,
                    position_key="p-2",
                    gross_yield_usd=Decimal("30"),
                    strategy_fee_usd=Decimal("4.5"),
                    avant_gop_usd=Decimal("2.55"),
                    net_yield_usd=Decimal("22.95"),
                    avg_equity_usd=Decimal("300"),
                    gross_roe=Decimal("0.1"),
                    post_strategy_fee_roe=Decimal("0.085"),
                    net_roe=Decimal("0.0765"),
                    avant_gop_roe=Decimal("0.0085"),
                    method=METHOD_APY_PRORATED_SOD_EOD,
                    confidence_score=Decimal("1"),
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        rollups = compute_window_rollups(session, window="7d", end_date=date(2026, 3, 3))

    assert rollups.total.gross_yield_usd == Decimal("50")
    assert rollups.total.avg_equity_usd == Decimal("500")
    assert rollups.total.gross_roe == Decimal("0.1")
    assert rollups.total.post_strategy_fee_roe == Decimal("0.085")
    assert rollups.total.net_roe == Decimal("0.0765")
    assert rollups.total.avant_gop_roe == Decimal("0.0085")
