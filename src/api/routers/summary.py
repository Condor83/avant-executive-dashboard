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
from api.schemas.summary import ExecutiveSummarySnapshot, HolderSummarySnapshot, SummaryResponse
from core.config import load_consumer_thresholds_config
from core.dashboard_contracts import leverage_ratio
from core.db.models import (
    ConsumerHolderUniverseDaily,
    DataQuality,
    ExecutiveSummaryDaily,
    HolderBehaviorDaily,
    HolderProductSegmentDaily,
    HolderScorecardDaily,
    HolderSupplyCoverageDaily,
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


def _portfolio_leverage_ratio(
    *,
    total_supply_usd: Decimal,
    total_net_equity_usd: Decimal,
) -> Decimal | None:
    return leverage_ratio(supply_usd=total_supply_usd, equity_usd=total_net_equity_usd)


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
        avg_leverage_ratio=_portfolio_leverage_ratio(
            total_supply_usd=row.total_supply_usd,
            total_net_equity_usd=row.total_net_equity_usd,
        ),
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
        holder_summary=None,
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

    holder_scorecard = session.get(HolderScorecardDaily, business_date)
    holder_summary = None
    if holder_scorecard is not None:
        thresholds = load_consumer_thresholds_config()
        whale_threshold = thresholds.whales.wallet_usd_threshold
        supply_coverage = session.scalar(
            select(HolderSupplyCoverageDaily).where(
                HolderSupplyCoverageDaily.business_date == business_date,
                HolderSupplyCoverageDaily.chain_code
                == thresholds.supply_coverage.primary_chain_code,
                HolderSupplyCoverageDaily.token_symbol
                == thresholds.supply_coverage.primary_token_symbol,
            )
        )
        holder_rows = session.scalars(
            select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        ).all()
        all_segment = session.scalar(
            select(HolderProductSegmentDaily).where(
                HolderProductSegmentDaily.business_date == business_date,
                HolderProductSegmentDaily.product_scope == "all",
                HolderProductSegmentDaily.cohort_segment == "all",
            )
        )
        core_segment = session.scalar(
            select(HolderProductSegmentDaily).where(
                HolderProductSegmentDaily.business_date == business_date,
                HolderProductSegmentDaily.product_scope == "all",
                HolderProductSegmentDaily.cohort_segment == "core",
            )
        )
        whale_segment = session.scalar(
            select(HolderProductSegmentDaily).where(
                HolderProductSegmentDaily.business_date == business_date,
                HolderProductSegmentDaily.product_scope == "all",
                HolderProductSegmentDaily.cohort_segment == "whale",
            )
        )
        whale_wallet_count = sum(
            1 for row in holder_rows if row.total_canonical_avant_exposure_usd >= whale_threshold
        )
        monitored_holder_count = (
            supply_coverage.monitoring_wallet_count
            if supply_coverage is not None
            else (
                session.scalar(
                    select(func.count())
                    .select_from(ConsumerHolderUniverseDaily)
                    .where(ConsumerHolderUniverseDaily.business_date == business_date)
                )
                or 0
            )
        )
        attributed_holder_count = (
            all_segment.holder_count
            if all_segment is not None
            else holder_scorecard.tracked_holders
        )
        core_holder_wallet_count = (
            core_segment.holder_count
            if core_segment is not None
            else holder_scorecard.tracked_holders
        )
        if whale_segment is not None:
            whale_wallet_count = whale_segment.holder_count
        attribution_completion_pct = (
            Decimal(attributed_holder_count) / Decimal(monitored_holder_count)
            if monitored_holder_count > 0
            else None
        )
        configured_deployed_share = (
            holder_scorecard.configured_deployed_avant_usd
            / holder_scorecard.total_canonical_avant_exposure_usd
            if holder_scorecard.total_canonical_avant_exposure_usd > ZERO
            else None
        )
        holder_summary = HolderSummarySnapshot(
            supply_coverage_token_symbol=(
                supply_coverage.token_symbol if supply_coverage is not None else None
            ),
            supply_coverage_chain_code=(
                supply_coverage.chain_code if supply_coverage is not None else None
            ),
            monitored_holder_count=int(monitored_holder_count),
            attributed_holder_count=int(attributed_holder_count),
            attribution_completion_pct=attribution_completion_pct,
            core_holder_wallet_count=int(core_holder_wallet_count),
            whale_wallet_count=int(whale_wallet_count),
            strategy_supply_usd=(
                supply_coverage.strategy_supply_usd if supply_coverage is not None else ZERO
            ),
            strategy_deployed_supply_usd=(
                supply_coverage.strategy_deployed_supply_usd
                if supply_coverage is not None
                else ZERO
            ),
            net_customer_float_usd=(
                supply_coverage.net_customer_float_usd if supply_coverage is not None else ZERO
            ),
            covered_supply_usd=(
                supply_coverage.covered_supply_usd if supply_coverage is not None else ZERO
            ),
            covered_supply_pct=(
                supply_coverage.covered_supply_pct if supply_coverage is not None else None
            ),
            cross_chain_supply_usd=(
                supply_coverage.cross_chain_supply_usd if supply_coverage is not None else ZERO
            ),
            total_observed_aum_usd=(
                all_segment.observed_aum_usd if all_segment is not None else ZERO
            ),
            total_canonical_avant_exposure_usd=holder_scorecard.total_canonical_avant_exposure_usd,
            whale_concentration_pct=(
                whale_segment.observed_aum_usd / all_segment.observed_aum_usd
                if whale_segment is not None
                and all_segment is not None
                and all_segment.observed_aum_usd > ZERO
                else None
            ),
            defi_active_pct=all_segment.defi_active_pct if all_segment is not None else None,
            avasset_deployed_pct=(
                all_segment.avasset_deployed_pct if all_segment is not None else None
            ),
            staked_share=holder_scorecard.staked_share,
            configured_deployed_share=configured_deployed_share,
            top10_holder_share=holder_scorecard.top10_holder_share,
            visibility_gap_wallet_count=holder_scorecard.visibility_gap_wallet_count,
            markets_needing_capacity_review=holder_scorecard.markets_needing_capacity_review,
        )

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
        holder_summary=holder_summary,
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
