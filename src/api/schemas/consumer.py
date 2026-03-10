"""Response schemas for holder dashboard and supplemental visibility endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class ConsumerKpiSummary(BaseModel):
    monitored_holder_count: int
    attributed_holder_count: int
    total_observed_aum_usd: Decimal
    whale_concentration_pct: Decimal | None
    whale_concentration_wallet_count: int
    whale_concentration_aum_usd: Decimal
    defi_active_pct: Decimal | None
    avasset_deployed_pct: Decimal | None
    verified_holder_count: int
    core_holder_count: int
    whale_holder_count: int


class ConsumerCoverageSummary(BaseModel):
    raw_holder_rows: int
    excluded_holder_rows: int
    monitored_holder_count: int
    attributed_holder_count: int
    attribution_completion_pct: Decimal | None


class ConsumerCohortCard(BaseModel):
    segment: str
    label: str
    threshold_label: str
    holder_count: int
    aum_usd: Decimal
    aum_share_pct: Decimal | None
    avg_holding_usd: Decimal | None
    median_age_days: int | None
    idle_usd: Decimal
    fixed_yield_pt_usd: Decimal
    yield_token_yt_usd: Decimal
    collateralized_usd: Decimal
    borrowed_usd: Decimal
    staked_usd: Decimal
    other_defi_usd: Decimal
    idle_pct: Decimal | None
    fixed_yield_pt_pct: Decimal | None
    yield_token_yt_pct: Decimal | None
    collateralized_pct: Decimal | None
    borrowed_against_pct: Decimal | None
    staked_pct: Decimal | None
    defi_active_pct: Decimal | None
    avasset_deployed_pct: Decimal | None
    conviction_gap_pct: Decimal | None
    multi_asset_pct: Decimal | None
    aum_change_7d_pct: Decimal | None
    new_wallet_count_7d: int
    exited_wallet_count_7d: int
    up_wallet_pct_7d: Decimal | None
    flat_wallet_pct_7d: Decimal | None
    down_wallet_pct_7d: Decimal | None


class ConsumerSummaryResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    kpis: ConsumerKpiSummary
    coverage: ConsumerCoverageSummary
    cohorts: list[ConsumerCohortCard]


class ConsumerBehaviorComparisonRow(BaseModel):
    segment: str
    label: str
    threshold_label: str
    holder_count: int
    aum_usd: Decimal
    avg_holding_usd: Decimal | None
    median_age_days: int | None
    idle_pct: Decimal | None
    collateralized_pct: Decimal | None
    borrowed_against_pct: Decimal | None
    staked_pct: Decimal | None
    defi_active_pct: Decimal | None
    avasset_deployed_pct: Decimal | None
    conviction_gap_pct: Decimal | None
    multi_asset_pct: Decimal | None
    aum_change_7d_pct: Decimal | None
    new_wallet_count_7d: int
    exited_wallet_count_7d: int


class ConsumerBehaviorComparisonResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    rows: list[ConsumerBehaviorComparisonRow]


class ConsumerAdoptionFunnelSegment(BaseModel):
    segment: str
    label: str
    threshold_label: str
    holder_count: int
    defi_active_holder_count: int
    avasset_deployed_holder_count: int
    conviction_gap_holder_count: int
    conviction_gap_pct: Decimal | None


class ConsumerAdoptionFunnelResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    cohorts: list[ConsumerAdoptionFunnelSegment]


class ConsumerDeploymentRow(BaseModel):
    protocol_code: str
    chain_code: str
    verified_wallet_count: int
    core_wallet_count: int
    whale_wallet_count: int
    total_value_usd: Decimal
    total_borrow_usd: Decimal
    dominant_token_symbols: list[str]
    primary_use: str


class ConsumerDeploymentsResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    total_deployed_value_usd: Decimal
    deployments: list[ConsumerDeploymentRow]


class ConsumerTopWalletRow(BaseModel):
    wallet_address: str
    segment: str
    asset_symbols: list[str]
    total_value_usd: Decimal
    wallet_held_usd: Decimal
    configured_deployed_usd: Decimal
    fixed_yield_pt_usd: Decimal
    yield_token_yt_usd: Decimal
    other_defi_usd: Decimal
    external_deployed_usd: Decimal
    borrowed_usd: Decimal
    leverage_ratio: Decimal | None
    health_factor_min: Decimal | None
    risk_band: str | None
    deployment_state: str
    aum_delta_7d_usd: Decimal | None
    aum_delta_7d_pct: Decimal | None
    is_signoff_eligible: bool
    behavior_tags: list[str]


class ConsumerTopWalletsResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    rank_mode: str
    total_count: int
    wallets: list[ConsumerTopWalletRow]


class ConsumerCapacityRow(BaseModel):
    market_id: int
    market_name: str
    protocol_code: str
    chain_code: str
    collateral_family: str
    holder_count: int
    collateral_wallet_count: int
    leveraged_wallet_count: int
    avant_collateral_usd: Decimal
    borrowed_usd: Decimal
    idle_eligible_same_chain_usd: Decimal
    p50_leverage_ratio: Decimal | None
    p90_leverage_ratio: Decimal | None
    top10_collateral_share: Decimal | None
    utilization: Decimal | None
    available_liquidity_usd: Decimal | None
    cap_headroom_usd: Decimal | None
    capacity_pressure_score: int
    needs_capacity_review: bool
    near_limit_wallet_count: int
    avant_collateral_usd_delta_7d: Decimal | None
    collateral_wallet_count_delta_7d: int | None


class ConsumerRiskCohortProfile(BaseModel):
    segment: str
    label: str
    threshold_label: str
    borrowed_against_pct: Decimal | None
    idle_pct: Decimal | None
    critical_or_elevated_wallet_count: int
    near_limit_wallet_count: int


class ConsumerRiskSignalsResponse(BaseModel):
    business_date: date
    product: str
    product_label: str
    cohort_profiles: list[ConsumerRiskCohortProfile]
    capacity_signals: list[ConsumerCapacityRow]


class ConsumerCapacityResponse(BaseModel):
    business_date: date
    total_count: int
    markets: list[ConsumerCapacityRow]


class ConsumerVisibilitySummaryResponse(BaseModel):
    business_date: date
    visibility_wallets: int
    seed_wallets: int
    verified_cohort_wallets: int
    signoff_cohort_wallets: int
    any_defi_activity_wallet_count: int
    any_defi_borrow_wallet_count: int
    configured_surface_wallet_count: int
    any_defi_activity_pct_visibility: Decimal | None
    any_defi_borrow_pct_visibility: Decimal | None
    configured_surface_activity_pct_visibility: Decimal | None
    signoff_any_defi_activity_pct: Decimal | None
    signoff_any_defi_borrow_pct: Decimal | None
    signoff_configured_surface_activity_pct: Decimal | None
    seed_only_wallet_count: int
    discovered_only_wallet_count: int
    visibility_gap_wallet_count: int


class ConsumerVisibilityProtocolGapRow(BaseModel):
    protocol_code: str
    wallet_count: int
    signoff_wallet_count: int
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    in_config_surface: bool
    gap_priority: int


class ConsumerVisibilityProtocolGapsResponse(BaseModel):
    business_date: date
    total_count: int
    protocols: list[ConsumerVisibilityProtocolGapRow]
