"""Holder dashboard endpoints backed by persisted consumer analytics tables."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from analytics.holder_dashboard_engine import (
    PRODUCT_SCOPE_LABELS,
    family_matches_token_symbol,
)
from api.deps import get_session
from api.schemas.consumer import (
    ConsumerAdoptionFunnelResponse,
    ConsumerAdoptionFunnelSegment,
    ConsumerBehaviorComparisonResponse,
    ConsumerBehaviorComparisonRow,
    ConsumerCapacityResponse,
    ConsumerCapacityRow,
    ConsumerCohortCard,
    ConsumerCoverageSummary,
    ConsumerDeploymentRow,
    ConsumerDeploymentsResponse,
    ConsumerKpiSummary,
    ConsumerRiskCohortProfile,
    ConsumerRiskSignalsResponse,
    ConsumerSummaryResponse,
    ConsumerTopWalletRow,
    ConsumerTopWalletsResponse,
    ConsumerVisibilityProtocolGapRow,
    ConsumerVisibilityProtocolGapsResponse,
    ConsumerVisibilitySummaryResponse,
)
from core.config import load_consumer_thresholds_config
from core.consumer_debank_visibility import is_excluded_visibility_protocol
from core.db.models import (
    ConsumerDebankWalletDaily,
    ConsumerMarketDemandDaily,
    ConsumerTokenHolderDaily,
    HolderBehaviorDaily,
    HolderProductSegmentDaily,
    HolderProtocolDeployDaily,
    HolderProtocolGapDaily,
    HolderWalletProductDaily,
    Market,
)

router = APIRouter(prefix="/consumer")

ZERO = Decimal("0")
PRODUCT_QUERY_PATTERN = "^(all|avusd|aveth|avbtc)$"
RISK_SEVERITY = {"critical": 4, "elevated": 3, "watch": 2, "normal": 1, "unknown": 0}
COHORT_SEGMENT_ORDER = ("verified", "core", "whale")
COHORT_LABELS = {
    "verified": ("Verified", "<$50k"),
    "core": ("Core", "$50k–$1M"),
    "whale": ("Whales", "$1M+"),
}


def _latest_dashboard_business_date(session: Session) -> date | None:
    latest = session.scalar(select(func.max(HolderProductSegmentDaily.business_date)))
    if latest is not None:
        return latest
    return session.scalar(select(func.max(HolderBehaviorDaily.business_date)))


def _latest_visibility_business_date(session: Session) -> date | None:
    latest = session.scalar(select(func.max(ConsumerDebankWalletDaily.business_date)))
    if latest is not None:
        return latest
    return session.scalar(select(func.max(HolderProtocolGapDaily.business_date)))


def _pct(count: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(count) / Decimal(denominator)


def _share(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator <= ZERO:
        return None
    return numerator / denominator


def _product_label(product: str) -> str:
    return PRODUCT_SCOPE_LABELS.get(product, product)


def _product_family(product: str) -> str | None:
    if product == "avusd":
        return "usd"
    if product == "aveth":
        return "eth"
    if product == "avbtc":
        return "btc"
    return None


def _load_segment_rows(
    session: Session,
    *,
    business_date: date,
    product: str,
) -> dict[str, HolderProductSegmentDaily]:
    rows = session.scalars(
        select(HolderProductSegmentDaily).where(
            HolderProductSegmentDaily.business_date == business_date,
            HolderProductSegmentDaily.product_scope == product,
        )
    ).all()
    return {row.cohort_segment: row for row in rows}


def _load_wallet_product_rows(
    session: Session,
    *,
    business_date: date,
    product: str,
) -> list[HolderWalletProductDaily]:
    return session.scalars(
        select(HolderWalletProductDaily).where(
            HolderWalletProductDaily.business_date == business_date,
            HolderWalletProductDaily.product_scope == product,
        )
    ).all()


def _coverage_summary(
    session: Session,
    *,
    business_date: date,
    product: str,
) -> ConsumerCoverageSummary:
    token_rows = session.scalars(
        select(ConsumerTokenHolderDaily).where(
            ConsumerTokenHolderDaily.business_date == business_date
        )
    ).all()
    family = _product_family(product)
    filtered_token_rows = [
        row
        for row in token_rows
        if family is None or family_matches_token_symbol(family, row.token_symbol)
    ]
    raw_holder_rows = len(filtered_token_rows)
    excluded_holder_rows = sum(1 for row in filtered_token_rows if row.exclude_from_monitoring)
    monitored_wallet_ids = {
        row.wallet_id for row in filtered_token_rows if not row.exclude_from_monitoring
    }
    attributed_wallet_ids = {
        row.wallet_id
        for row in _load_wallet_product_rows(session, business_date=business_date, product=product)
        if row.is_attributed
    }
    monitored_holder_count = len(monitored_wallet_ids)
    attributed_holder_count = len(attributed_wallet_ids)
    return ConsumerCoverageSummary(
        raw_holder_rows=raw_holder_rows,
        excluded_holder_rows=excluded_holder_rows,
        monitored_holder_count=monitored_holder_count,
        attributed_holder_count=attributed_holder_count,
        attribution_completion_pct=_pct(attributed_holder_count, monitored_holder_count),
    )


def _movement_pct_from_wallet_rows(
    *,
    wallet_rows: list[HolderWalletProductDaily],
    thresholds,
    direction: str,
) -> Decimal | None:
    if not wallet_rows:
        return None
    moved = 0
    for wallet in wallet_rows:
        delta = wallet.aum_delta_7d_usd
        if direction == "up" and delta > thresholds.classification_dust_floor_usd:
            moved += 1
        elif direction == "flat" and abs(delta) <= thresholds.classification_dust_floor_usd:
            moved += 1
        elif direction == "down" and delta < -thresholds.classification_dust_floor_usd:
            moved += 1
    return _pct(moved, len(wallet_rows))


def _cohort_card(
    *,
    row: HolderProductSegmentDaily,
    total_aum_usd: Decimal,
    wallet_rows: list[HolderWalletProductDaily],
    thresholds,
) -> ConsumerCohortCard:
    label, threshold_label = COHORT_LABELS[row.cohort_segment]
    return ConsumerCohortCard(
        segment=row.cohort_segment,
        label=label,
        threshold_label=threshold_label,
        holder_count=row.holder_count,
        aum_usd=row.observed_aum_usd,
        aum_share_pct=_share(row.observed_aum_usd, total_aum_usd),
        avg_holding_usd=row.avg_holding_usd,
        median_age_days=row.median_age_days,
        idle_usd=row.idle_usd,
        fixed_yield_pt_usd=row.fixed_yield_pt_usd,
        yield_token_yt_usd=row.yield_token_yt_usd,
        collateralized_usd=row.collateralized_usd,
        borrowed_usd=row.borrowed_usd,
        staked_usd=row.staked_usd,
        other_defi_usd=row.other_defi_usd,
        idle_pct=row.idle_pct,
        fixed_yield_pt_pct=row.fixed_yield_pt_pct,
        yield_token_yt_pct=_share(row.yield_token_yt_usd, row.observed_aum_usd),
        collateralized_pct=row.collateralized_pct,
        borrowed_against_pct=row.borrowed_against_pct,
        staked_pct=row.staked_pct,
        defi_active_pct=row.defi_active_pct,
        avasset_deployed_pct=row.avasset_deployed_pct,
        conviction_gap_pct=row.conviction_gap_pct,
        multi_asset_pct=row.multi_asset_pct,
        aum_change_7d_pct=row.aum_change_7d_pct,
        new_wallet_count_7d=row.new_wallet_count_7d,
        exited_wallet_count_7d=row.exited_wallet_count_7d,
        up_wallet_pct_7d=_movement_pct_from_wallet_rows(
            wallet_rows=wallet_rows,
            thresholds=thresholds,
            direction="up",
        ),
        flat_wallet_pct_7d=_movement_pct_from_wallet_rows(
            wallet_rows=wallet_rows,
            thresholds=thresholds,
            direction="flat",
        ),
        down_wallet_pct_7d=_movement_pct_from_wallet_rows(
            wallet_rows=wallet_rows,
            thresholds=thresholds,
            direction="down",
        ),
    )


def _capacity_rows(
    session: Session,
    *,
    business_date: date,
    product: str,
) -> list[ConsumerCapacityRow]:
    query = (
        select(ConsumerMarketDemandDaily, Market.display_name)
        .join(Market, Market.market_id == ConsumerMarketDemandDaily.market_id)
        .where(ConsumerMarketDemandDaily.business_date == business_date)
        .order_by(
            ConsumerMarketDemandDaily.needs_capacity_review.desc(),
            ConsumerMarketDemandDaily.capacity_pressure_score.desc(),
            ConsumerMarketDemandDaily.utilization.desc().nullslast(),
            ConsumerMarketDemandDaily.avant_collateral_usd.desc(),
            Market.display_name.asc(),
        )
    )
    family = _product_family(product)
    if family is not None:
        query = query.where(ConsumerMarketDemandDaily.collateral_family == family)
    rows = []
    for demand_row, display_name in session.execute(query).all():
        rows.append(
            ConsumerCapacityRow(
                market_id=demand_row.market_id,
                market_name=str(display_name or demand_row.market_id),
                protocol_code=demand_row.protocol_code,
                chain_code=demand_row.chain_code,
                collateral_family=demand_row.collateral_family,
                holder_count=demand_row.holder_count,
                collateral_wallet_count=demand_row.collateral_wallet_count,
                leveraged_wallet_count=demand_row.leveraged_wallet_count,
                avant_collateral_usd=demand_row.avant_collateral_usd,
                borrowed_usd=demand_row.borrowed_usd,
                idle_eligible_same_chain_usd=demand_row.idle_eligible_same_chain_usd,
                p50_leverage_ratio=demand_row.p50_leverage_ratio,
                p90_leverage_ratio=demand_row.p90_leverage_ratio,
                top10_collateral_share=demand_row.top10_collateral_share,
                utilization=demand_row.utilization,
                available_liquidity_usd=demand_row.available_liquidity_usd,
                cap_headroom_usd=demand_row.cap_headroom_usd,
                capacity_pressure_score=demand_row.capacity_pressure_score,
                needs_capacity_review=demand_row.needs_capacity_review,
                near_limit_wallet_count=demand_row.near_limit_wallet_count,
                avant_collateral_usd_delta_7d=demand_row.avant_collateral_usd_delta_7d,
                collateral_wallet_count_delta_7d=demand_row.collateral_wallet_count_delta_7d,
            )
        )
    return rows


def _deployment_state(
    *,
    borrowed: Decimal,
    collateral: Decimal,
    fixed_yield: Decimal,
    yield_token: Decimal,
    other_defi: Decimal,
    thresholds,
) -> str:
    if borrowed > thresholds.leveraged_borrow_usd_floor:
        return "Borrowed"
    if collateral >= thresholds.classification_dust_floor_usd:
        return "Collateralized"
    if fixed_yield >= thresholds.classification_dust_floor_usd:
        return "Fixed Yield"
    if yield_token >= thresholds.classification_dust_floor_usd:
        return "Yield Token"
    if other_defi >= thresholds.classification_dust_floor_usd:
        return "Deployed"
    return "Idle"


@router.get("/summary")
def get_consumer_summary(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    session: Session = Depends(get_session),
) -> ConsumerSummaryResponse:
    thresholds = load_consumer_thresholds_config()
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerSummaryResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            kpis=ConsumerKpiSummary(
                monitored_holder_count=0,
                attributed_holder_count=0,
                total_observed_aum_usd=ZERO,
                whale_concentration_pct=None,
                whale_concentration_wallet_count=0,
                whale_concentration_aum_usd=ZERO,
                defi_active_pct=None,
                avasset_deployed_pct=None,
                verified_holder_count=0,
                core_holder_count=0,
                whale_holder_count=0,
            ),
            coverage=ConsumerCoverageSummary(
                raw_holder_rows=0,
                excluded_holder_rows=0,
                monitored_holder_count=0,
                attributed_holder_count=0,
                attribution_completion_pct=None,
            ),
            cohorts=[],
        )

    segment_rows = _load_segment_rows(session, business_date=business_date, product=product)
    all_row = segment_rows.get("all")
    if all_row is None:
        return ConsumerSummaryResponse(
            business_date=business_date,
            product=product,
            product_label=_product_label(product),
            kpis=ConsumerKpiSummary(
                monitored_holder_count=0,
                attributed_holder_count=0,
                total_observed_aum_usd=ZERO,
                whale_concentration_pct=None,
                whale_concentration_wallet_count=0,
                whale_concentration_aum_usd=ZERO,
                defi_active_pct=None,
                avasset_deployed_pct=None,
                verified_holder_count=0,
                core_holder_count=0,
                whale_holder_count=0,
            ),
            coverage=_coverage_summary(session, business_date=business_date, product=product),
            cohorts=[],
        )

    coverage = _coverage_summary(session, business_date=business_date, product=product)
    wallet_product_rows = _load_wallet_product_rows(
        session,
        business_date=business_date,
        product=product,
    )
    wallet_rows_by_segment = {
        segment: [row for row in wallet_product_rows if row.segment == segment]
        for segment in COHORT_SEGMENT_ORDER
    }
    whale_row = segment_rows.get("whale")
    cohorts = [
        _cohort_card(
            row=segment_rows[segment],
            total_aum_usd=all_row.observed_aum_usd,
            wallet_rows=wallet_rows_by_segment.get(segment, []),
            thresholds=thresholds,
        )
        for segment in COHORT_SEGMENT_ORDER
        if segment in segment_rows
    ]
    return ConsumerSummaryResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        kpis=ConsumerKpiSummary(
            monitored_holder_count=coverage.monitored_holder_count,
            attributed_holder_count=coverage.attributed_holder_count,
            total_observed_aum_usd=all_row.observed_aum_usd,
            whale_concentration_pct=(
                _share(whale_row.observed_aum_usd, all_row.observed_aum_usd)
                if whale_row is not None
                else None
            ),
            whale_concentration_wallet_count=whale_row.holder_count if whale_row is not None else 0,
            whale_concentration_aum_usd=(
                whale_row.observed_aum_usd if whale_row is not None else ZERO
            ),
            defi_active_pct=all_row.defi_active_pct,
            avasset_deployed_pct=all_row.avasset_deployed_pct,
            verified_holder_count=segment_rows.get("verified").holder_count
            if segment_rows.get("verified") is not None
            else 0,
            core_holder_count=segment_rows.get("core").holder_count
            if segment_rows.get("core") is not None
            else 0,
            whale_holder_count=whale_row.holder_count if whale_row is not None else 0,
        ),
        coverage=coverage,
        cohorts=cohorts,
    )


@router.get("/behavior-comparison")
def get_consumer_behavior_comparison(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    session: Session = Depends(get_session),
) -> ConsumerBehaviorComparisonResponse:
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerBehaviorComparisonResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            rows=[],
        )
    segment_rows = _load_segment_rows(session, business_date=business_date, product=product)
    rows = []
    for segment in COHORT_SEGMENT_ORDER:
        row = segment_rows.get(segment)
        if row is None:
            continue
        label, threshold_label = COHORT_LABELS[segment]
        rows.append(
            ConsumerBehaviorComparisonRow(
                segment=segment,
                label=label,
                threshold_label=threshold_label,
                holder_count=row.holder_count,
                aum_usd=row.observed_aum_usd,
                avg_holding_usd=row.avg_holding_usd,
                median_age_days=row.median_age_days,
                idle_pct=row.idle_pct,
                collateralized_pct=row.collateralized_pct,
                borrowed_against_pct=row.borrowed_against_pct,
                staked_pct=row.staked_pct,
                defi_active_pct=row.defi_active_pct,
                avasset_deployed_pct=row.avasset_deployed_pct,
                conviction_gap_pct=row.conviction_gap_pct,
                multi_asset_pct=row.multi_asset_pct,
                aum_change_7d_pct=row.aum_change_7d_pct,
                new_wallet_count_7d=row.new_wallet_count_7d,
                exited_wallet_count_7d=row.exited_wallet_count_7d,
            )
        )
    return ConsumerBehaviorComparisonResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        rows=rows,
    )


@router.get("/adoption-funnel")
def get_consumer_adoption_funnel(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    session: Session = Depends(get_session),
) -> ConsumerAdoptionFunnelResponse:
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerAdoptionFunnelResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            cohorts=[],
        )
    segment_rows = _load_segment_rows(session, business_date=business_date, product=product)
    cohorts = []
    for segment in COHORT_SEGMENT_ORDER:
        row = segment_rows.get(segment)
        if row is None:
            continue
        label, threshold_label = COHORT_LABELS[segment]
        cohorts.append(
            ConsumerAdoptionFunnelSegment(
                segment=segment,
                label=label,
                threshold_label=threshold_label,
                holder_count=row.holder_count,
                defi_active_holder_count=row.defi_active_wallet_count,
                avasset_deployed_holder_count=row.avasset_deployed_wallet_count,
                conviction_gap_holder_count=row.conviction_gap_wallet_count,
                conviction_gap_pct=row.conviction_gap_pct,
            )
        )
    return ConsumerAdoptionFunnelResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        cohorts=cohorts,
    )


@router.get("/deployments")
def get_consumer_deployments(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    limit: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> ConsumerDeploymentsResponse:
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerDeploymentsResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            total_deployed_value_usd=ZERO,
            deployments=[],
        )
    rows = session.scalars(
        select(HolderProtocolDeployDaily)
        .where(
            HolderProtocolDeployDaily.business_date == business_date,
            HolderProtocolDeployDaily.product_scope == product,
        )
        .order_by(
            HolderProtocolDeployDaily.whale_wallet_count.desc(),
            HolderProtocolDeployDaily.core_wallet_count.desc(),
            HolderProtocolDeployDaily.total_value_usd.desc(),
            HolderProtocolDeployDaily.protocol_code.asc(),
            HolderProtocolDeployDaily.chain_code.asc(),
        )
    ).all()
    deployments = [
        ConsumerDeploymentRow(
            protocol_code=row.protocol_code,
            chain_code=row.chain_code,
            verified_wallet_count=row.verified_wallet_count,
            core_wallet_count=row.core_wallet_count,
            whale_wallet_count=row.whale_wallet_count,
            total_value_usd=row.total_value_usd,
            total_borrow_usd=row.total_borrow_usd,
            dominant_token_symbols=list(row.dominant_token_symbols_json or []),
            primary_use=row.primary_use,
        )
        for row in rows[:limit]
    ]
    return ConsumerDeploymentsResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        total_deployed_value_usd=sum((row.total_value_usd for row in rows), ZERO),
        deployments=deployments,
    )


@router.get("/top-wallets")
def get_consumer_top_wallets(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    rank: str = Query(default="assets", pattern="^(assets|borrow|risk)$"),
    limit: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> ConsumerTopWalletsResponse:
    thresholds = load_consumer_thresholds_config()
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerTopWalletsResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            rank_mode=rank,
            total_count=0,
            wallets=[],
        )
    holder_rows = {
        row.wallet_id: row
        for row in session.scalars(
            select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        ).all()
    }
    wallet_rows = [
        row
        for row in _load_wallet_product_rows(session, business_date=business_date, product=product)
        if row.is_attributed and row.segment is not None
    ]
    eligible_wallets = []
    for wallet in wallet_rows:
        holder_row = holder_rows.get(wallet.wallet_id)
        external_deployed = (
            wallet.external_fixed_yield_pt_usd
            + wallet.external_yield_token_yt_usd
            + wallet.external_other_defi_usd
        )
        eligible_wallets.append(
            ConsumerTopWalletRow(
                wallet_address=wallet.wallet_address,
                segment=str(wallet.segment),
                asset_symbols=list(wallet.asset_symbols_json or []),
                total_value_usd=wallet.observed_exposure_usd,
                wallet_held_usd=wallet.wallet_held_usd,
                configured_deployed_usd=wallet.canonical_deployed_usd,
                fixed_yield_pt_usd=wallet.external_fixed_yield_pt_usd,
                yield_token_yt_usd=wallet.external_yield_token_yt_usd,
                other_defi_usd=wallet.external_other_defi_usd,
                external_deployed_usd=external_deployed,
                borrowed_usd=wallet.borrowed_usd,
                leverage_ratio=wallet.leverage_ratio,
                health_factor_min=wallet.health_factor_min,
                risk_band=wallet.risk_band,
                deployment_state=_deployment_state(
                    borrowed=wallet.borrowed_usd,
                    collateral=wallet.canonical_deployed_usd,
                    fixed_yield=wallet.external_fixed_yield_pt_usd,
                    yield_token=wallet.external_yield_token_yt_usd,
                    other_defi=wallet.external_other_defi_usd,
                    thresholds=thresholds,
                ),
                aum_delta_7d_usd=wallet.aum_delta_7d_usd,
                aum_delta_7d_pct=wallet.aum_delta_7d_pct,
                is_signoff_eligible=(
                    holder_row.is_signoff_eligible if holder_row is not None else False
                ),
                behavior_tags=(
                    list(holder_row.behavior_tags_json or []) if holder_row is not None else []
                ),
            )
        )
    if rank == "assets":
        eligible_wallets.sort(
            key=lambda row: (-row.total_value_usd, row.wallet_address),
        )
    elif rank == "borrow":
        eligible_wallets.sort(
            key=lambda row: (-row.borrowed_usd, -row.total_value_usd, row.wallet_address),
        )
    else:
        eligible_wallets.sort(
            key=lambda row: (
                -RISK_SEVERITY.get((row.risk_band or "unknown").lower(), 0),
                Decimal("999999999") if row.health_factor_min is None else row.health_factor_min,
                -row.borrowed_usd,
                -row.total_value_usd,
                row.wallet_address,
            ),
        )
    return ConsumerTopWalletsResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        rank_mode=rank,
        total_count=len(eligible_wallets),
        wallets=eligible_wallets[:limit],
    )


@router.get("/risk-signals")
def get_consumer_risk_signals(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    session: Session = Depends(get_session),
) -> ConsumerRiskSignalsResponse:
    thresholds = load_consumer_thresholds_config()
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerRiskSignalsResponse(
            business_date=datetime.now(UTC).date(),
            product=product,
            product_label=_product_label(product),
            cohort_profiles=[],
            capacity_signals=[],
        )
    wallet_product_rows = _load_wallet_product_rows(
        session,
        business_date=business_date,
        product=product,
    )
    profiles = []
    for segment in COHORT_SEGMENT_ORDER:
        label, threshold_label = COHORT_LABELS[segment]
        wallets = [row for row in wallet_product_rows if row.segment == segment]
        if not wallets:
            continue
        borrowed_against_count = sum(
            1 for wallet in wallets if wallet.borrowed_usd > thresholds.leveraged_borrow_usd_floor
        )
        critical_or_elevated = sum(
            1 for wallet in wallets if wallet.risk_band in {"critical", "elevated"}
        )
        near_limit_count = sum(
            1
            for wallet in wallets
            if wallet.health_factor_min is not None
            and wallet.health_factor_min < thresholds.capacity.near_limit_health_factor_threshold
        )
        profiles.append(
            ConsumerRiskCohortProfile(
                segment=segment,
                label=label,
                threshold_label=threshold_label,
                borrowed_against_pct=_pct(borrowed_against_count, len(wallets)),
                idle_pct=_share(
                    sum((wallet.wallet_held_usd for wallet in wallets), ZERO),
                    sum((wallet.observed_exposure_usd for wallet in wallets), ZERO),
                ),
                critical_or_elevated_wallet_count=critical_or_elevated,
                near_limit_wallet_count=near_limit_count,
            )
        )
    return ConsumerRiskSignalsResponse(
        business_date=business_date,
        product=product,
        product_label=_product_label(product),
        cohort_profiles=profiles,
        capacity_signals=_capacity_rows(session, business_date=business_date, product=product),
    )


@router.get("/markets/capacity")
def get_consumer_capacity(
    product: str = Query(default="all", pattern=PRODUCT_QUERY_PATTERN),
    session: Session = Depends(get_session),
) -> ConsumerCapacityResponse:
    business_date = _latest_dashboard_business_date(session)
    if business_date is None:
        return ConsumerCapacityResponse(
            business_date=datetime.now(UTC).date(),
            total_count=0,
            markets=[],
        )
    rows = _capacity_rows(session, business_date=business_date, product=product)
    return ConsumerCapacityResponse(
        business_date=business_date,
        total_count=len(rows),
        markets=rows,
    )


@router.get("/visibility/summary")
def get_consumer_visibility_summary(
    session: Session = Depends(get_session),
) -> ConsumerVisibilitySummaryResponse:
    business_date = _latest_visibility_business_date(session)
    if business_date is None:
        return ConsumerVisibilitySummaryResponse(
            business_date=datetime.now(UTC).date(),
            visibility_wallets=0,
            seed_wallets=0,
            verified_cohort_wallets=0,
            signoff_cohort_wallets=0,
            any_defi_activity_wallet_count=0,
            any_defi_borrow_wallet_count=0,
            configured_surface_wallet_count=0,
            any_defi_activity_pct_visibility=None,
            any_defi_borrow_pct_visibility=None,
            configured_surface_activity_pct_visibility=None,
            signoff_any_defi_activity_pct=None,
            signoff_any_defi_borrow_pct=None,
            signoff_configured_surface_activity_pct=None,
            seed_only_wallet_count=0,
            discovered_only_wallet_count=0,
            visibility_gap_wallet_count=0,
        )
    rows = session.scalars(
        select(ConsumerDebankWalletDaily).where(
            ConsumerDebankWalletDaily.business_date == business_date
        )
    ).all()
    visibility_wallets = len(rows)
    seed_wallets = sum(1 for row in rows if row.in_seed_set)
    verified_cohort_wallets = sum(1 for row in rows if row.in_verified_cohort)
    signoff_cohort_wallets = sum(1 for row in rows if row.in_signoff_cohort)
    any_defi_activity_wallets = sum(1 for row in rows if row.has_any_activity)
    any_defi_borrow_wallets = sum(1 for row in rows if row.has_any_borrow)
    configured_surface_wallets = sum(1 for row in rows if row.has_configured_surface_activity)
    signoff_activity_wallets = sum(
        1 for row in rows if row.in_signoff_cohort and row.has_any_activity
    )
    signoff_borrow_wallets = sum(1 for row in rows if row.in_signoff_cohort and row.has_any_borrow)
    signoff_configured_wallets = sum(
        1 for row in rows if row.in_signoff_cohort and row.has_configured_surface_activity
    )
    seed_only_wallet_count = sum(
        1 for row in rows if row.in_seed_set and not row.in_verified_cohort
    )
    discovered_only_wallet_count = sum(
        1 for row in rows if row.in_verified_cohort and not row.in_seed_set
    )
    visibility_gap_wallet_count = sum(
        1 for row in rows if row.has_any_activity and not row.has_configured_surface_activity
    )
    return ConsumerVisibilitySummaryResponse(
        business_date=business_date,
        visibility_wallets=visibility_wallets,
        seed_wallets=seed_wallets,
        verified_cohort_wallets=verified_cohort_wallets,
        signoff_cohort_wallets=signoff_cohort_wallets,
        any_defi_activity_wallet_count=any_defi_activity_wallets,
        any_defi_borrow_wallet_count=any_defi_borrow_wallets,
        configured_surface_wallet_count=configured_surface_wallets,
        any_defi_activity_pct_visibility=_pct(any_defi_activity_wallets, visibility_wallets),
        any_defi_borrow_pct_visibility=_pct(any_defi_borrow_wallets, visibility_wallets),
        configured_surface_activity_pct_visibility=_pct(
            configured_surface_wallets,
            visibility_wallets,
        ),
        signoff_any_defi_activity_pct=_pct(signoff_activity_wallets, signoff_cohort_wallets),
        signoff_any_defi_borrow_pct=_pct(signoff_borrow_wallets, signoff_cohort_wallets),
        signoff_configured_surface_activity_pct=_pct(
            signoff_configured_wallets,
            signoff_cohort_wallets,
        ),
        seed_only_wallet_count=seed_only_wallet_count,
        discovered_only_wallet_count=discovered_only_wallet_count,
        visibility_gap_wallet_count=visibility_gap_wallet_count,
    )


@router.get("/visibility/protocol-gaps")
def get_consumer_visibility_protocol_gaps(
    session: Session = Depends(get_session),
) -> ConsumerVisibilityProtocolGapsResponse:
    business_date = session.scalar(select(func.max(HolderProtocolGapDaily.business_date)))
    if business_date is None:
        return ConsumerVisibilityProtocolGapsResponse(
            business_date=datetime.now(UTC).date(),
            total_count=0,
            protocols=[],
        )
    protocols = [
        ConsumerVisibilityProtocolGapRow(
            protocol_code=row.protocol_code,
            wallet_count=row.wallet_count,
            signoff_wallet_count=row.signoff_wallet_count,
            total_supply_usd=row.total_supply_usd,
            total_borrow_usd=row.total_borrow_usd,
            in_config_surface=row.in_config_surface,
            gap_priority=row.gap_priority,
        )
        for row in session.scalars(
            select(HolderProtocolGapDaily)
            .where(HolderProtocolGapDaily.business_date == business_date)
            .order_by(
                HolderProtocolGapDaily.gap_priority.asc(),
                HolderProtocolGapDaily.protocol_code.asc(),
            )
        ).all()
        if not is_excluded_visibility_protocol(row.protocol_code)
    ]
    return ConsumerVisibilityProtocolGapsResponse(
        business_date=business_date,
        total_count=len(protocols),
        protocols=protocols,
    )
