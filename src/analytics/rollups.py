"""Windowed rollups derived from persisted daily position rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from analytics.yield_engine import METHOD_APY_PRORATED_SOD_EOD, compute_roe_breakdown
from core.db.models import YieldDaily

ZERO = Decimal("0")
SUPPORTED_WINDOWS: dict[str, int] = {"7d": 7, "30d": 30}


@dataclass(frozen=True)
class RollupMetrics:
    """Yield, fee, and net totals for a grouping."""

    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal
    avg_equity_usd: Decimal
    gross_roe: Decimal | None
    post_strategy_fee_roe: Decimal | None
    net_roe: Decimal | None
    avant_gop_roe: Decimal | None


@dataclass(frozen=True)
class GroupRollup:
    """Windowed rollup for a specific entity ID."""

    entity_id: int
    metrics: RollupMetrics


@dataclass(frozen=True)
class WindowRollups:
    """Rollup result container for CLI and tests."""

    window: str
    start_date: date | None
    end_date: date | None
    wallet_rollups: list[GroupRollup]
    product_rollups: list[GroupRollup]
    protocol_rollups: list[GroupRollup]
    total: RollupMetrics


def parse_window(window: str) -> int:
    """Translate a human window (7d/30d) into days."""

    days = SUPPORTED_WINDOWS.get(window.lower())
    if days is None:
        raise ValueError(
            f"unsupported rollup window '{window}' (expected one of {sorted(SUPPORTED_WINDOWS)})"
        )
    return days


def compute_window_rollups(
    session: Session,
    *,
    window: str,
    end_date: date | None = None,
) -> WindowRollups:
    """Compute wallet/product/protocol/total rollups for a trailing window."""

    window_days = parse_window(window)
    effective_end_date = end_date or session.scalar(
        select(func.max(YieldDaily.business_date)).where(
            YieldDaily.position_key.is_not(None),
            YieldDaily.method == METHOD_APY_PRORATED_SOD_EOD,
        )
    )
    if effective_end_date is None:
        return WindowRollups(
            window=window,
            start_date=None,
            end_date=None,
            wallet_rollups=[],
            product_rollups=[],
            protocol_rollups=[],
            total=RollupMetrics(
                gross_yield_usd=ZERO,
                strategy_fee_usd=ZERO,
                avant_gop_usd=ZERO,
                net_yield_usd=ZERO,
                avg_equity_usd=ZERO,
                gross_roe=None,
                post_strategy_fee_roe=None,
                net_roe=None,
                avant_gop_roe=None,
            ),
        )

    start_date = effective_end_date - timedelta(days=window_days - 1)
    filters: tuple[ColumnElement[bool], ...] = (
        YieldDaily.method == METHOD_APY_PRORATED_SOD_EOD,
        YieldDaily.position_key.is_not(None),
        YieldDaily.business_date >= start_date,
        YieldDaily.business_date <= effective_end_date,
    )

    wallet_rollups = _query_wallet_rollups(session=session, filters=filters)
    product_rollups = _query_product_rollups(session=session, filters=filters)
    protocol_rollups = _query_protocol_rollups(session=session, filters=filters)

    total_row = session.execute(
        select(
            func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avg_equity_usd), ZERO),
        ).where(*filters)
    ).one()
    total_roe = compute_roe_breakdown(
        gross_yield_usd=total_row[0],
        strategy_fee_usd=total_row[1],
        net_yield_usd=total_row[3],
        avant_gop_usd=total_row[2],
        avg_equity_usd=total_row[4],
    )
    total = RollupMetrics(
        gross_yield_usd=total_row[0],
        strategy_fee_usd=total_row[1],
        avant_gop_usd=total_row[2],
        net_yield_usd=total_row[3],
        avg_equity_usd=total_roe.avg_equity_usd,
        gross_roe=total_roe.gross_roe,
        post_strategy_fee_roe=total_roe.post_strategy_fee_roe,
        net_roe=total_roe.net_roe,
        avant_gop_roe=total_roe.avant_gop_roe,
    )

    return WindowRollups(
        window=window,
        start_date=start_date,
        end_date=effective_end_date,
        wallet_rollups=wallet_rollups,
        product_rollups=product_rollups,
        protocol_rollups=protocol_rollups,
        total=total,
    )


def _query_wallet_rollups(
    *,
    session: Session,
    filters: tuple[ColumnElement[bool], ...],
) -> list[GroupRollup]:
    rows = session.execute(
        select(
            YieldDaily.wallet_id,
            func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avg_equity_usd), ZERO),
        )
        .where(*filters)
        .where(YieldDaily.wallet_id.is_not(None))
        .group_by(YieldDaily.wallet_id)
        .order_by(YieldDaily.wallet_id.asc())
    ).all()

    return [GroupRollup(entity_id=row[0], metrics=_build_metrics_from_row(row)) for row in rows]


def _query_product_rollups(
    *,
    session: Session,
    filters: tuple[ColumnElement[bool], ...],
) -> list[GroupRollup]:
    rows = session.execute(
        select(
            YieldDaily.product_id,
            func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avg_equity_usd), ZERO),
        )
        .where(*filters)
        .where(YieldDaily.product_id.is_not(None))
        .group_by(YieldDaily.product_id)
        .order_by(YieldDaily.product_id.asc())
    ).all()

    return [GroupRollup(entity_id=row[0], metrics=_build_metrics_from_row(row)) for row in rows]


def _query_protocol_rollups(
    *,
    session: Session,
    filters: tuple[ColumnElement[bool], ...],
) -> list[GroupRollup]:
    rows = session.execute(
        select(
            YieldDaily.protocol_id,
            func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
            func.coalesce(func.sum(YieldDaily.avg_equity_usd), ZERO),
        )
        .where(*filters)
        .where(YieldDaily.protocol_id.is_not(None))
        .group_by(YieldDaily.protocol_id)
        .order_by(YieldDaily.protocol_id.asc())
    ).all()

    return [GroupRollup(entity_id=row[0], metrics=_build_metrics_from_row(row)) for row in rows]


def _build_metrics_from_row(
    row: Row[tuple[int | None, Decimal, Decimal, Decimal, Decimal, Decimal | None]],
) -> RollupMetrics:
    avg_equity_usd = row[5] if row[5] is not None else ZERO
    roe = compute_roe_breakdown(
        gross_yield_usd=row[1],
        strategy_fee_usd=row[2],
        net_yield_usd=row[4],
        avant_gop_usd=row[3],
        avg_equity_usd=avg_equity_usd,
    )
    return RollupMetrics(
        gross_yield_usd=row[1],
        strategy_fee_usd=row[2],
        avant_gop_usd=row[3],
        net_yield_usd=row[4],
        avg_equity_usd=roe.avg_equity_usd,
        gross_roe=roe.gross_roe,
        post_strategy_fee_roe=roe.post_strategy_fee_roe,
        net_roe=roe.net_roe,
        avant_gop_roe=roe.avant_gop_roe,
    )
