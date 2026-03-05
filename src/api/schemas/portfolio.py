"""Response schemas for /portfolio/* endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.common import YieldMetrics


class ProductRow(BaseModel):
    product_id: int
    product_code: str
    yesterday: YieldMetrics
    trailing_7d: YieldMetrics
    trailing_30d: YieldMetrics


class PositionRow(BaseModel):
    position_key: str
    wallet_address: str
    product_code: str | None
    protocol_code: str
    chain_code: str
    market_address: str
    supplied_usd: Decimal
    borrowed_usd: Decimal
    equity_usd: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    reward_apy: Decimal
    health_factor: Decimal | None
    ltv: Decimal | None
    gross_yield_usd: Decimal | None
    net_yield_usd: Decimal | None
    gross_roe: Decimal | None


class PaginatedPositions(BaseModel):
    as_of_date: date
    total_count: int
    page: int
    page_size: int
    positions: list[PositionRow]
