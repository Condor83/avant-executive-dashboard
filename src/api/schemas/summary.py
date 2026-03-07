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


class SummaryResponse(BaseModel):
    business_date: date
    executive: ExecutiveSummarySnapshot
    portfolio_summary: PortfolioSummaryResponse | None
    market_summary: MarketSummaryResponse | None
    freshness: FreshnessSummary
