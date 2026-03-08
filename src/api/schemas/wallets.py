"""Response schemas for wallet dashboard endpoints."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class WalletSummaryRow(BaseModel):
    wallet_address: str
    wallet_label: str | None
    product_code: str | None
    product_label: str | None
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    total_tvl_usd: Decimal


class WalletsResponse(BaseModel):
    business_date: date
    total_count: int
    wallets: list[WalletSummaryRow]
