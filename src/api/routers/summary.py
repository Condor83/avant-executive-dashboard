"""Executive summary endpoints backed by served portfolio and markets tables."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.common import FreshnessSummary
from api.schemas.markets import MarketSummaryResponse
from api.schemas.portfolio import PortfolioSummaryResponse
from api.schemas.summary import ExecutiveSummarySnapshot, SummaryResponse
from core.db.models import (
    DataQuality,
    ExecutiveSummaryDaily,
    MarketSnapshot,
    MarketSummaryDaily,
    PortfolioSummaryDaily,
    PositionSnapshot,
)

router = APIRouter(prefix="/summary")

ZERO = Decimal("0")
ANNUALIZATION_DAYS = Decimal("365")
ROE_QUANTUM = Decimal("0.0000000001")


def _normalized_roe(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(ROE_QUANTUM)


def _annualize_daily_roe(value: Decimal | None) -> Decimal | None:
    normalized = _normalized_roe(value)
    if normalized is None:
        return None
    return normalized * ANNUALIZATION_DAYS


def _freshness(session: Session) -> FreshnessSummary:
    now = datetime.now(UTC)
    latest_ps_ts = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
    latest_ms_ts = session.scalar(select(func.max(MarketSnapshot.as_of_ts_utc)))
    pos_age = (now - latest_ps_ts).total_seconds() / 3600 if latest_ps_ts else None
    mkt_age = (now - latest_ms_ts).total_seconds() / 3600 if latest_ms_ts else None
    issue_count_24h = (
        session.scalar(
            select(func.count())
            .select_from(DataQuality)
            .where(DataQuality.created_at >= now - timedelta(hours=24))
        )
        or 0
    )
    return FreshnessSummary(
        last_position_snapshot_utc=latest_ps_ts,
        last_market_snapshot_utc=latest_ms_ts,
        position_snapshot_age_hours=round(pos_age, 2) if pos_age is not None else None,
        market_snapshot_age_hours=round(mkt_age, 2) if mkt_age is not None else None,
        open_dq_issues_24h=int(issue_count_24h),
    )


def _portfolio_summary(session: Session, business_date: date) -> PortfolioSummaryResponse | None:
    row = session.scalar(
        select(PortfolioSummaryDaily).where(
            PortfolioSummaryDaily.business_date == business_date,
            PortfolioSummaryDaily.scope_segment == "strategy_only",
        )
    )
    if row is None:
        return None
    return PortfolioSummaryResponse(
        business_date=row.business_date,
        scope_segment=row.scope_segment,
        total_supply_usd=row.total_supply_usd,
        total_borrow_usd=row.total_borrow_usd,
        total_net_equity_usd=row.total_net_equity_usd,
        aggregate_roe_daily=_normalized_roe(row.aggregate_roe),
        aggregate_roe_annualized=_annualize_daily_roe(row.aggregate_roe),
        total_gross_yield_daily_usd=row.total_gross_yield_daily_usd,
        total_net_yield_daily_usd=row.total_net_yield_daily_usd,
        total_gross_yield_mtd_usd=row.total_gross_yield_mtd_usd,
        total_net_yield_mtd_usd=row.total_net_yield_mtd_usd,
        total_strategy_fee_daily_usd=row.total_strategy_fee_daily_usd,
        total_avant_gop_daily_usd=row.total_avant_gop_daily_usd,
        total_strategy_fee_mtd_usd=row.total_strategy_fee_mtd_usd,
        total_avant_gop_mtd_usd=row.total_avant_gop_mtd_usd,
        avg_leverage_ratio=row.avg_leverage_ratio,
        open_position_count=row.open_position_count,
    )


def _market_summary(session: Session, business_date: date) -> MarketSummaryResponse | None:
    row = session.scalar(
        select(MarketSummaryDaily).where(
            MarketSummaryDaily.business_date == business_date,
            MarketSummaryDaily.scope_segment == "strategy_only",
        )
    )
    if row is None:
        return None
    return MarketSummaryResponse(
        business_date=row.business_date,
        scope_segment=row.scope_segment,
        total_supply_usd=row.total_supply_usd,
        total_borrow_usd=row.total_borrow_usd,
        weighted_utilization=row.weighted_utilization,
        total_available_liquidity_usd=row.total_available_liquidity_usd,
        markets_at_risk_count=row.markets_at_risk_count,
        markets_on_watchlist_count=row.markets_on_watchlist_count,
    )


def _empty_summary(today: date) -> SummaryResponse:
    return SummaryResponse(
        business_date=today,
        executive=ExecutiveSummarySnapshot(
            business_date=today,
            nav_usd=ZERO,
            portfolio_net_equity_usd=ZERO,
            market_stability_ops_net_equity_usd=ZERO,
            portfolio_aggregate_roe_daily=None,
            portfolio_aggregate_roe_annualized=None,
            total_gross_yield_daily_usd=ZERO,
            total_net_yield_daily_usd=ZERO,
            total_gross_yield_mtd_usd=ZERO,
            total_net_yield_mtd_usd=ZERO,
            total_strategy_fee_daily_usd=ZERO,
            total_avant_gop_daily_usd=ZERO,
            total_strategy_fee_mtd_usd=ZERO,
            total_avant_gop_mtd_usd=ZERO,
            market_total_supply_usd=ZERO,
            market_total_borrow_usd=ZERO,
            markets_at_risk_count=0,
            open_alert_count=0,
            customer_metrics_ready=False,
        ),
        portfolio_summary=None,
        market_summary=None,
        freshness=FreshnessSummary(
            last_position_snapshot_utc=None,
            last_market_snapshot_utc=None,
            position_snapshot_age_hours=None,
            market_snapshot_age_hours=None,
            open_dq_issues_24h=0,
        ),
    )


def _summary_payload(session: Session) -> SummaryResponse:
    business_date = session.scalar(select(func.max(ExecutiveSummaryDaily.business_date)))
    if business_date is None:
        return _empty_summary(datetime.now(UTC).date())

    executive = session.get(ExecutiveSummaryDaily, business_date)
    if executive is None:
        return _empty_summary(business_date)

    return SummaryResponse(
        business_date=business_date,
        executive=ExecutiveSummarySnapshot(
            business_date=executive.business_date,
            nav_usd=executive.nav_usd,
            portfolio_net_equity_usd=executive.portfolio_net_equity_usd,
            market_stability_ops_net_equity_usd=executive.market_stability_ops_net_equity_usd,
            portfolio_aggregate_roe_daily=_normalized_roe(executive.portfolio_aggregate_roe),
            portfolio_aggregate_roe_annualized=_annualize_daily_roe(
                executive.portfolio_aggregate_roe
            ),
            total_gross_yield_daily_usd=executive.total_gross_yield_daily_usd,
            total_net_yield_daily_usd=executive.total_net_yield_daily_usd,
            total_gross_yield_mtd_usd=executive.total_gross_yield_mtd_usd,
            total_net_yield_mtd_usd=executive.total_net_yield_mtd_usd,
            total_strategy_fee_daily_usd=executive.total_strategy_fee_daily_usd,
            total_avant_gop_daily_usd=executive.total_avant_gop_daily_usd,
            total_strategy_fee_mtd_usd=executive.total_strategy_fee_mtd_usd,
            total_avant_gop_mtd_usd=executive.total_avant_gop_mtd_usd,
            market_total_supply_usd=executive.market_total_supply_usd,
            market_total_borrow_usd=executive.market_total_borrow_usd,
            markets_at_risk_count=executive.markets_at_risk_count,
            open_alert_count=executive.open_alert_count,
            customer_metrics_ready=executive.customer_metrics_ready,
        ),
        portfolio_summary=_portfolio_summary(session, business_date),
        market_summary=_market_summary(session, business_date),
        freshness=_freshness(session),
    )


@router.get("")
def get_summary(session: Session = Depends(get_session)) -> SummaryResponse:
    return _summary_payload(session)


@router.get("/executive")
def get_executive_summary(session: Session = Depends(get_session)) -> SummaryResponse:
    return _summary_payload(session)
