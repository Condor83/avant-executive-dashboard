"""GET /summary — top-level executive dashboard snapshot."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from analytics.rollups import RollupMetrics, compute_window_rollups
from api.deps import get_session
from api.schemas.common import YieldMetrics
from api.schemas.summary import (
    DataQualitySummary,
    PortfolioSnapshot,
    SummaryResponse,
)
from core.db.models import (
    DataQuality,
    MarketSnapshot,
    PositionSnapshot,
    Wallet,
    YieldDaily,
)

router = APIRouter()

ZERO = Decimal("0")


def _rollup_to_yield_metrics(m: RollupMetrics) -> YieldMetrics:
    return YieldMetrics(
        gross_yield_usd=m.gross_yield_usd,
        strategy_fee_usd=m.strategy_fee_usd,
        avant_gop_usd=m.avant_gop_usd,
        net_yield_usd=m.net_yield_usd,
        avg_equity_usd=m.avg_equity_usd,
        gross_roe=m.gross_roe,
        net_roe=m.net_roe,
    )


def _yesterday_yield(session: Session) -> tuple[YieldMetrics, date | None]:
    """Return yesterday's total yield and the business_date used."""
    latest_date = session.scalar(
        select(func.max(YieldDaily.business_date)).where(
            YieldDaily.position_key.is_(None),
            YieldDaily.wallet_id.is_(None),
            YieldDaily.product_id.is_(None),
            YieldDaily.protocol_id.is_(None),
        )
    )
    if latest_date is None:
        return YieldMetrics(
            gross_yield_usd=ZERO,
            strategy_fee_usd=ZERO,
            avant_gop_usd=ZERO,
            net_yield_usd=ZERO,
            avg_equity_usd=ZERO,
            gross_roe=None,
            net_roe=None,
        ), None

    row = session.execute(
        select(YieldDaily).where(
            YieldDaily.business_date == latest_date,
            YieldDaily.position_key.is_(None),
            YieldDaily.wallet_id.is_(None),
            YieldDaily.product_id.is_(None),
            YieldDaily.protocol_id.is_(None),
        )
    ).scalar_one()

    return YieldMetrics(
        gross_yield_usd=row.gross_yield_usd,
        strategy_fee_usd=row.strategy_fee_usd,
        avant_gop_usd=row.avant_gop_usd,
        net_yield_usd=row.net_yield_usd,
        avg_equity_usd=row.avg_equity_usd or ZERO,
        gross_roe=row.gross_roe,
        net_roe=row.net_roe,
    ), latest_date


@router.get("/summary")
def get_summary(session: Session = Depends(get_session)) -> SummaryResponse:
    # 1. Portfolio snapshot from latest position_snapshots
    latest_ps_ts = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
    if latest_ps_ts is not None:
        portfolio_row = session.execute(
            select(
                func.coalesce(func.sum(PositionSnapshot.supplied_usd), 0),
                func.coalesce(func.sum(PositionSnapshot.borrowed_usd), 0),
                func.coalesce(func.sum(PositionSnapshot.equity_usd), 0),
            )
            .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
            .where(
                Wallet.wallet_type == "strategy",
                PositionSnapshot.as_of_ts_utc == latest_ps_ts,
            )
        ).one()
        supplied = portfolio_row[0]
        borrowed = portfolio_row[1]
        equity = portfolio_row[2]
    else:
        supplied = ZERO
        borrowed = ZERO
        equity = ZERO

    portfolio = PortfolioSnapshot(
        total_supplied_usd=supplied,
        total_borrowed_usd=borrowed,
        net_equity_usd=equity,
        collateralization_ratio=supplied / borrowed if borrowed else None,
        leverage_ratio=supplied / equity if equity else None,
    )

    # 2. Yesterday yield
    yield_yesterday, latest_date = _yesterday_yield(session)

    # 3. Trailing windows
    rollup_7d = compute_window_rollups(session, window="7d")
    rollup_30d = compute_window_rollups(session, window="30d")

    # 4. Data quality summary
    now = datetime.now(UTC)
    latest_ms_ts = session.scalar(select(func.max(MarketSnapshot.as_of_ts_utc)))
    dq_count = (
        session.scalar(
            select(func.count())
            .select_from(DataQuality)
            .where(DataQuality.created_at >= now - timedelta(hours=24))
        )
        or 0
    )

    pos_age = (now - latest_ps_ts).total_seconds() / 3600 if latest_ps_ts else None
    mkt_age = (now - latest_ms_ts).total_seconds() / 3600 if latest_ms_ts else None

    return SummaryResponse(
        as_of_date=latest_date or date.today(),
        portfolio=portfolio,
        yield_yesterday=yield_yesterday,
        yield_trailing_7d=_rollup_to_yield_metrics(rollup_7d.total),
        yield_trailing_30d=_rollup_to_yield_metrics(rollup_30d.total),
        data_quality=DataQualitySummary(
            last_position_snapshot_utc=latest_ps_ts,
            last_market_snapshot_utc=latest_ms_ts,
            position_snapshot_age_hours=round(pos_age, 2) if pos_age is not None else None,
            market_snapshot_age_hours=round(mkt_age, 2) if mkt_age is not None else None,
            open_dq_issues_24h=dq_count,
        ),
    )
