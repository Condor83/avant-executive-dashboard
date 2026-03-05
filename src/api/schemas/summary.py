"""Response schema for GET /summary."""

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel

from api.schemas.common import YieldMetrics


class PortfolioSnapshot(BaseModel):
    total_supplied_usd: Decimal
    total_borrowed_usd: Decimal
    net_equity_usd: Decimal
    collateralization_ratio: Decimal | None
    leverage_ratio: Decimal | None


class DataQualitySummary(BaseModel):
    last_position_snapshot_utc: datetime | None
    last_market_snapshot_utc: datetime | None
    position_snapshot_age_hours: float | None
    market_snapshot_age_hours: float | None
    open_dq_issues_24h: int


class SummaryResponse(BaseModel):
    as_of_date: date
    portfolio: PortfolioSnapshot
    yield_yesterday: YieldMetrics
    yield_trailing_7d: YieldMetrics
    yield_trailing_30d: YieldMetrics
    data_quality: DataQualitySummary
