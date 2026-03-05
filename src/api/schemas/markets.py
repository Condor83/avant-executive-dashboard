"""Response schemas for /markets/* endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.alerts import AlertRow


class MarketOverviewRow(BaseModel):
    market_id: int
    protocol_code: str
    chain_code: str
    market_address: str
    base_asset_symbol: str | None
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    utilization: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    spread_apy: Decimal
    available_liquidity_usd: Decimal | None
    avant_supplied_usd: Decimal
    avant_borrowed_usd: Decimal
    avant_supply_share: Decimal | None
    avant_borrow_share: Decimal | None
    max_ltv: Decimal | None
    liquidation_threshold: Decimal | None
    liquidation_penalty: Decimal | None
    open_alert_count: int


class MarketHistoryPoint(BaseModel):
    business_date: date
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    utilization: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    spread_apy: Decimal
    avant_supplied_usd: Decimal
    avant_borrowed_usd: Decimal


class WatchlistRow(MarketOverviewRow):
    alerts: list[AlertRow]
