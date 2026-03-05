"""Shared ingestion datatypes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class PriceRequest:
    """Token price lookup request."""

    token_id: int
    chain_code: str
    address_or_mint: str
    symbol: str


@dataclass(frozen=True)
class PriceQuote:
    """Normalized USD price quote."""

    token_id: int
    chain_code: str
    address_or_mint: str
    price_usd: Decimal
    source: str = "defillama"


@dataclass(frozen=True)
class PositionSnapshotInput:
    """Adapter output used for position snapshot persistence."""

    as_of_ts_utc: datetime
    protocol_code: str
    chain_code: str
    wallet_address: str
    market_ref: str
    position_key: str
    supplied_amount: Decimal
    supplied_usd: Decimal
    borrowed_amount: Decimal
    borrowed_usd: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    reward_apy: Decimal
    equity_usd: Decimal
    source: str = "rpc"
    block_number_or_slot: str | None = None
    health_factor: Decimal | None = None
    ltv: Decimal | None = None


@dataclass(frozen=True)
class DataQualityIssue:
    """Standard failure payload written into `data_quality`."""

    as_of_ts_utc: datetime
    stage: str
    error_type: str
    error_message: str
    protocol_code: str | None = None
    chain_code: str | None = None
    wallet_address: str | None = None
    market_ref: str | None = None
    payload_json: dict[str, Any] | None = None


@dataclass(frozen=True)
class MarketSnapshotInput:
    """Adapter output used for market snapshot persistence."""

    as_of_ts_utc: datetime
    protocol_code: str
    chain_code: str
    market_ref: str
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    utilization: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    source: str = "rpc"
    block_number_or_slot: str | None = None
    available_liquidity_usd: Decimal | None = None
    max_ltv: Decimal | None = None
    liquidation_threshold: Decimal | None = None
    liquidation_penalty: Decimal | None = None
    caps_json: dict[str, Any] | None = None
    irm_params_json: dict[str, Any] | None = None
