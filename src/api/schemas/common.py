"""Shared response shapes reused across multiple endpoints."""

from decimal import Decimal

from pydantic import BaseModel


class YieldMetrics(BaseModel):
    """Yield, fee, and ROE metrics for a time window."""

    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal
    avg_equity_usd: Decimal
    gross_roe: Decimal | None
    net_roe: Decimal | None
