"""Fee waterfall utilities for daily yield analytics."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

STRATEGY_FEE_RATE = Decimal("0.15")
AVANT_GOP_REMAINDER_RATE = Decimal("0.10")
AVANT_GOP_EFFECTIVE_RATE = Decimal("0.085")
NET_TO_USERS_RATE = Decimal("0.765")


@dataclass(frozen=True)
class FeeBreakdown:
    """Computed fee waterfall outputs for a gross-yield amount."""

    gross_yield_usd: Decimal
    strategy_fee_usd: Decimal
    avant_gop_usd: Decimal
    net_yield_usd: Decimal


def apply_fee_waterfall(gross_yield_usd: Decimal) -> FeeBreakdown:
    """Apply the fixed performance-fee waterfall to a gross yield amount."""

    if gross_yield_usd <= Decimal("0"):
        return FeeBreakdown(
            gross_yield_usd=gross_yield_usd,
            strategy_fee_usd=Decimal("0"),
            avant_gop_usd=Decimal("0"),
            net_yield_usd=gross_yield_usd,
        )

    strategy_fee = gross_yield_usd * STRATEGY_FEE_RATE
    remainder = gross_yield_usd - strategy_fee
    avant_gop = remainder * AVANT_GOP_REMAINDER_RATE
    net_yield = remainder - avant_gop

    return FeeBreakdown(
        gross_yield_usd=gross_yield_usd,
        strategy_fee_usd=strategy_fee,
        avant_gop_usd=avant_gop,
        net_yield_usd=net_yield,
    )
