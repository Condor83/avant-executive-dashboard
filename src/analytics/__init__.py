"""Analytics and rollup computation modules."""

from analytics.fee_engine import (
    AVANT_GOP_EFFECTIVE_RATE,
    NET_TO_USERS_RATE,
    STRATEGY_FEE_RATE,
    apply_fee_waterfall,
)
from analytics.rollups import compute_window_rollups
from analytics.yield_engine import YieldEngine

__all__ = [
    "AVANT_GOP_EFFECTIVE_RATE",
    "NET_TO_USERS_RATE",
    "STRATEGY_FEE_RATE",
    "YieldEngine",
    "apply_fee_waterfall",
    "compute_window_rollups",
]
