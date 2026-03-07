"""Response schemas for markets dashboard endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.alerts import AlertRow


class MarketExposureRow(BaseModel):
    market_exposure_id: int
    exposure_slug: str
    display_name: str
    protocol_code: str
    chain_code: str
    supply_symbol: str | None
    debt_symbol: str | None
    collateral_symbol: str | None
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    weighted_supply_apy: Decimal
    weighted_borrow_apy: Decimal
    utilization: Decimal
    available_liquidity_usd: Decimal
    distance_to_kink: Decimal | None
    strategy_position_count: int
    customer_position_count: int
    active_alert_count: int
    risk_status: str
    watch_status: str


class MarketExposureHistoryPoint(BaseModel):
    business_date: date
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    weighted_supply_apy: Decimal
    weighted_borrow_apy: Decimal
    utilization: Decimal
    available_liquidity_usd: Decimal
    distance_to_kink: Decimal | None
    active_alert_count: int
    risk_status: str


class NativeMarketComponent(BaseModel):
    market_id: int
    display_name: str
    market_kind: str
    protocol_code: str
    chain_code: str
    base_asset_symbol: str | None
    collateral_symbol: str | None
    current_total_supply_usd: Decimal | None
    current_total_borrow_usd: Decimal | None
    current_utilization: Decimal | None
    current_supply_apy: Decimal | None
    current_borrow_apy: Decimal | None
    current_available_liquidity_usd: Decimal | None
    current_distance_to_kink: Decimal | None
    active_alert_count: int


class MarketExposureDetailResponse(BaseModel):
    exposure: MarketExposureRow
    history: list[MarketExposureHistoryPoint]
    components: list[NativeMarketComponent]
    alerts: list[AlertRow]


class NativeMarketDetailResponse(BaseModel):
    component: NativeMarketComponent
    history: list[MarketExposureHistoryPoint]


class MarketSummaryResponse(BaseModel):
    business_date: date
    scope_segment: str
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    weighted_utilization: Decimal | None
    total_available_liquidity_usd: Decimal
    markets_at_risk_count: int
    markets_on_watchlist_count: int
