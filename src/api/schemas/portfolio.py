"""Response schemas for portfolio dashboard endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.common import RoeMetrics, YieldWindow


class PositionLeg(BaseModel):
    token_id: int | None
    symbol: str | None
    amount: Decimal
    usd_value: Decimal
    apy: Decimal
    estimated_daily_cashflow_usd: Decimal


class PortfolioPositionRow(BaseModel):
    position_id: int
    position_key: str
    display_name: str
    wallet_address: str
    wallet_label: str | None
    product_code: str | None
    product_label: str | None
    protocol_code: str
    chain_code: str
    position_kind: str
    market_exposure_slug: str | None
    supply_leg: PositionLeg
    supply_legs: list[PositionLeg]
    borrow_legs: list[PositionLeg]
    borrow_leg: PositionLeg | None
    net_equity_usd: Decimal
    leverage_ratio: Decimal | None
    health_factor: Decimal | None
    roe: RoeMetrics
    yield_daily: YieldWindow
    yield_mtd: YieldWindow


class PortfolioPositionHistoryPoint(BaseModel):
    business_date: date
    supply_usd: Decimal
    borrow_usd: Decimal
    net_equity_usd: Decimal
    leverage_ratio: Decimal | None
    health_factor: Decimal | None
    gross_yield_usd: Decimal
    net_yield_usd: Decimal
    roe: RoeMetrics


class PortfolioPositionsResponse(BaseModel):
    business_date: date
    total_count: int
    positions: list[PortfolioPositionRow]


class PortfolioSummaryResponse(BaseModel):
    business_date: date
    scope_segment: str
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    total_net_equity_usd: Decimal
    aggregate_roe_daily: Decimal | None
    aggregate_roe_annualized: Decimal | None
    total_gross_yield_daily_usd: Decimal
    total_net_yield_daily_usd: Decimal
    total_gross_yield_mtd_usd: Decimal
    total_net_yield_mtd_usd: Decimal
    total_strategy_fee_daily_usd: Decimal
    total_avant_gop_daily_usd: Decimal
    total_strategy_fee_mtd_usd: Decimal
    total_avant_gop_mtd_usd: Decimal
    avg_leverage_ratio: Decimal | None
    open_position_count: int


class PortfolioPositionDetailResponse(BaseModel):
    position: PortfolioPositionRow
    history: list[PortfolioPositionHistoryPoint]
