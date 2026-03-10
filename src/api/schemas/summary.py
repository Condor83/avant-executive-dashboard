"""Response schemas for executive summary endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.common import FreshnessSummary
from api.schemas.markets import MarketSummaryResponse
from api.schemas.portfolio import PortfolioSummaryResponse


class ExecutiveSummarySnapshot(BaseModel):
    business_date: date
    nav_usd: Decimal
    portfolio_net_equity_usd: Decimal
    market_stability_ops_net_equity_usd: Decimal
    portfolio_aggregate_roe_daily: Decimal | None
    portfolio_aggregate_roe_annualized: Decimal | None
    total_gross_yield_daily_usd: Decimal
    total_net_yield_daily_usd: Decimal
    total_gross_yield_mtd_usd: Decimal
    total_net_yield_mtd_usd: Decimal
    total_strategy_fee_daily_usd: Decimal
    total_avant_gop_daily_usd: Decimal
    total_strategy_fee_mtd_usd: Decimal
    total_avant_gop_mtd_usd: Decimal
    market_total_supply_usd: Decimal
    market_total_borrow_usd: Decimal
    markets_at_risk_count: int
    open_alert_count: int
    customer_metrics_ready: bool


class HolderSummarySnapshot(BaseModel):
    supply_coverage_token_symbol: str | None
    supply_coverage_chain_code: str | None
    monitored_holder_count: int
    attributed_holder_count: int
    attribution_completion_pct: Decimal | None
    core_holder_wallet_count: int
    whale_wallet_count: int
    strategy_supply_usd: Decimal
    strategy_deployed_supply_usd: Decimal
    net_customer_float_usd: Decimal
    covered_supply_usd: Decimal
    covered_supply_pct: Decimal | None
    cross_chain_supply_usd: Decimal
    total_observed_aum_usd: Decimal
    total_canonical_avant_exposure_usd: Decimal
    whale_concentration_pct: Decimal | None
    defi_active_pct: Decimal | None
    avasset_deployed_pct: Decimal | None
    staked_share: Decimal | None
    configured_deployed_share: Decimal | None
    top10_holder_share: Decimal | None
    visibility_gap_wallet_count: int
    markets_needing_capacity_review: int


class SummaryResponse(BaseModel):
    business_date: date
    executive: ExecutiveSummarySnapshot
    holder_summary: HolderSummarySnapshot | None
    portfolio_summary: PortfolioSummaryResponse | None
    market_summary: MarketSummaryResponse | None
    freshness: FreshnessSummary
