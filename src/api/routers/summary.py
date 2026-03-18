"""Executive summary endpoints backed by served portfolio and markets tables."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.common import FreshnessSummary
from api.schemas.markets import MarketSummaryResponse
from api.schemas.portfolio import PortfolioSummaryResponse
from api.schemas.summary import (
    ExecutiveSummarySnapshot,
    HolderSummarySnapshot,
    ProductPerformanceItem,
    ProtocolConcentrationItem,
    SummaryResponse,
)
from core.config import load_consumer_thresholds_config
from core.dashboard_contracts import (
    PRODUCT_BENCHMARK_TOKEN_MAP,
    code_label,
    leverage_ratio,
    product_label,
)
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
    PortfolioPositionCurrent,
    PortfolioSummaryDaily,
    PositionSnapshot,
    Product,
    Protocol,
    YieldDaily,
)
from core.settings import get_settings
from core.yields import AvantYieldOracle

router = APIRouter(prefix="/summary")
logger = logging.getLogger(__name__)

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


def _fetch_benchmark_map() -> dict[str, Decimal]:
    """Fetch benchmark APYs keyed by product_code. Returns empty dict on failure."""
    settings = get_settings()
    oracle = AvantYieldOracle(
        base_url=settings.avant_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    try:
        result: dict[str, Decimal] = {}
        seen_symbols: set[str] = set()
        for product_code, symbol in PRODUCT_BENCHMARK_TOKEN_MAP.items():
            if symbol in seen_symbols:
                # Reuse already-fetched APY for same symbol
                for prev_code, prev_sym in PRODUCT_BENCHMARK_TOKEN_MAP.items():
                    if prev_sym == symbol and prev_code in result:
                        result[product_code] = result[prev_code]
                        break
                continue
            try:
                apy = oracle.get_token_apy(symbol)
                result[product_code] = apy
                seen_symbols.add(symbol)
            except Exception:
                logger.warning("failed to fetch benchmark APY for %s", symbol, exc_info=True)
        return result
    except Exception:
        logger.warning("failed to fetch benchmarks", exc_info=True)
        return {}
    finally:
        oracle.close()


PRODUCT_SORT_ORDER = [
    "stablecoin_senior",
    "stablecoin_junior",
    "eth_senior",
    "eth_junior",
    "btc_senior",
    "btc_junior",
]


def _product_performance(
    session: Session, business_date: date
) -> list[ProductPerformanceItem] | None:
    """Build per-product ROE items from yield_daily product rollup rows."""
    rows = session.execute(
        select(YieldDaily, Product.product_code)
        .join(Product, Product.product_id == YieldDaily.product_id)
        .where(
            YieldDaily.business_date == business_date,
            YieldDaily.row_key.like("product:%"),
            YieldDaily.method == "apy_prorated_sod_eod",
        )
    ).all()

    benchmark_map = _fetch_benchmark_map()

    # Index by product_code for backfill
    yield_by_product: dict[str, YieldDaily] = {}
    for yield_row, product_code in rows:
        yield_by_product[product_code] = yield_row

    # If no yield data AND no benchmarks, nothing to show
    if not yield_by_product and not benchmark_map:
        return None

    items: list[ProductPerformanceItem] = []
    for code in PRODUCT_SORT_ORDER:
        yield_row = yield_by_product.get(code)
        items.append(
            ProductPerformanceItem(
                product_code=code,
                product_label=product_label(code) or code,
                gross_roe_daily=_normalized_roe(yield_row.gross_roe) if yield_row else None,
                gross_roe_annualized=(
                    _annualize_daily_roe(yield_row.gross_roe) if yield_row else None
                ),
                avg_equity_usd=yield_row.avg_equity_usd if yield_row else None,
                gross_yield_daily_usd=yield_row.gross_yield_usd if yield_row else ZERO,
                net_yield_daily_usd=yield_row.net_yield_usd if yield_row else ZERO,
                benchmark_apy=benchmark_map.get(code),
            )
        )
    return items


MARKET_STABILITY_OPS_PROTOCOLS = {"traderjoe_lp", "etherex", "wallet_balances"}


def _protocol_concentration(
    session: Session, business_date: date
) -> list[ProtocolConcentrationItem] | None:
    """Build protocol concentration breakdown from portfolio_positions_current."""
    rows = session.execute(
        select(
            Protocol.protocol_code,
            func.sum(PortfolioPositionCurrent.net_equity_usd).label("total_equity"),
        )
        .join(Protocol, Protocol.protocol_id == PortfolioPositionCurrent.protocol_id)
        .where(
            PortfolioPositionCurrent.business_date == business_date,
            PortfolioPositionCurrent.scope_segment == "strategy_only",
            Protocol.protocol_code.notin_(MARKET_STABILITY_OPS_PROTOCOLS),
        )
        .group_by(Protocol.protocol_code)
        .having(func.sum(PortfolioPositionCurrent.net_equity_usd) > 0)
    ).all()

    if not rows:
        return None

    grand_total = sum(row.total_equity for row in rows)
    if grand_total <= ZERO:
        return None

    sorted_rows = sorted(rows, key=lambda r: r.total_equity, reverse=True)
    return [
        ProtocolConcentrationItem(
            protocol_code=row.protocol_code,
            protocol_label=code_label(row.protocol_code) or row.protocol_code,
            net_equity_usd=row.total_equity,
            share_pct=row.total_equity / grand_total,
        )
        for row in sorted_rows
    ]


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
        product_performance=None,
        protocol_concentration=None,
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
            session.scalar(
                select(func.count())
                .select_from(ConsumerHolderUniverseDaily)
                .where(ConsumerHolderUniverseDaily.business_date == business_date)
            )
            or 0
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
        product_performance=_product_performance(session, business_date),
        protocol_concentration=_protocol_concentration(session, business_date),
        freshness=_freshness(session),
    )


@router.get("")
def get_summary(session: Session = Depends(get_session)) -> SummaryResponse:
    return _summary_payload(session)


@router.get("/executive")
def get_executive_summary(session: Session = Depends(get_session)) -> SummaryResponse:
    return _summary_payload(session)
