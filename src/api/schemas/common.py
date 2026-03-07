"""Shared response shapes reused across multiple endpoints."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class YieldMetrics(BaseModel):
    """Yield and fee metrics for a time window."""

    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal
    avg_equity_usd: Decimal


class RoeMetrics(BaseModel):
    """ROE metrics with explicit daily and annualized semantics."""

    gross_roe_daily: Decimal | None
    gross_roe_annualized: Decimal | None
    net_roe_daily: Decimal | None
    net_roe_annualized: Decimal | None


class YieldWindow(BaseModel):
    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal


class OptionItem(BaseModel):
    value: str
    label: str


class FreshnessSummary(BaseModel):
    last_position_snapshot_utc: datetime | None
    last_market_snapshot_utc: datetime | None
    position_snapshot_age_hours: float | None
    market_snapshot_age_hours: float | None
    open_dq_issues_24h: int
