"""Canonical dashboard-facing labels and normalization helpers."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

ZERO = Decimal("0")

PRODUCT_LABELS = {
    "stablecoin_senior": "savUSD (Senior Stable)",
    "stablecoin_junior": "avUSDx (Junior Stable)",
    "eth_senior": "savETH (Senior ETH)",
    "eth_junior": "avETHx (Junior ETH)",
    "btc_senior": "savBTC (Senior BTC)",
    "btc_junior": "avBTCx (Junior BTC)",
}

ALERT_SEVERITY_LABELS = {
    "low": "Low",
    "med": "Medium",
    "high": "High",
}

ALERT_STATUS_LABELS = {
    "open": "Open",
    "ack": "Acknowledged",
    "resolved": "Resolved",
}

POSITION_SORT_OPTIONS = [
    ("net_equity_usd", "Net Equity"),
    ("supply_usd", "Supply USD"),
    ("borrow_usd", "Borrow USD"),
    ("gross_yield_daily_usd", "Gross Yield (1D)"),
    ("net_yield_daily_usd", "Daily Net Yield"),
    ("strategy_fee_daily_usd", "Daily Performance Fee"),
    ("avant_gop_daily_usd", "Daily GOP"),
    ("gross_roe", "ROE"),
    ("net_roe", "Net ROE (Ann.)"),
    ("health_factor", "Health Factor"),
]

CODE_LABEL_OVERRIDES = {
    "aave_v3": "Aave V3",
    "pendle": "Pendle",
    "spark": "Spark",
}


def product_label(product_code: str | None) -> str | None:
    if product_code is None:
        return None
    return PRODUCT_LABELS.get(product_code, product_code.replace("_", " ").title())


def code_label(value: str | None) -> str | None:
    if value is None:
        return None
    return CODE_LABEL_OVERRIDES.get(value, value.replace("_", " ").title())


def alert_severity_label(value: str) -> str:
    return ALERT_SEVERITY_LABELS.get(value, value.title())


def alert_status_label(value: str) -> str:
    return ALERT_STATUS_LABELS.get(value, value.replace("_", " ").title())


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return slug.strip("-") or "item"


def position_exposure_class(metadata_json: Any, protocol_code: str) -> str:
    if isinstance(metadata_json, dict):
        raw = metadata_json.get("exposure_class")
        if raw == "idle_capital":
            return "idle_cash"
        if raw in {"ops_buy_wall", "market_stability_ops"}:
            return "ops"
        if raw in {"lp", "liquidity_pool"}:
            return "lp"
        if raw in {"core_lending", "idle_cash", "ops", "other"}:
            return raw
        if protocol_code == "stakedao":
            return "core_lending"
        if metadata_json.get("include_in_yield") is False:
            return "other"
    if protocol_code in {"wallet_balances"}:
        return "idle_cash"
    if protocol_code in {"traderjoe_lp", "etherex", "stakedao"}:
        return "ops"
    return "core_lending"


def market_kind(metadata_json: Any) -> str:
    if isinstance(metadata_json, dict):
        raw = metadata_json.get("kind")
        if isinstance(raw, str) and raw:
            return raw
    return "other"


def market_display_name(
    *,
    protocol_code: str,
    base_symbol: str | None,
    collateral_symbol: str | None,
    metadata_json: Any,
    market_address: str,
) -> str:
    kind = market_kind(metadata_json)
    if kind == "reserve":
        if base_symbol:
            return f"{base_symbol} Reserve"
    elif kind in {"market", "consumer_market"}:
        if collateral_symbol and base_symbol:
            return f"{collateral_symbol} / {base_symbol}"
        if base_symbol:
            return base_symbol
    elif kind == "vault":
        if isinstance(metadata_json, dict):
            for key in ("symbol", "asset_symbol", "name", "note"):
                value = metadata_json.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        if base_symbol:
            return f"{base_symbol} Vault"
    elif kind == "wallet_balance_token" and base_symbol:
        return f"Idle {base_symbol}"
    elif kind in {"liquidity_book_pool", "concentrated_liquidity_pool"}:
        token0 = collateral_symbol or "Token0"
        token1 = base_symbol or "Token1"
        return f"{token0} / {token1} Pool"
    elif kind == "vault_underlying" and base_symbol:
        return f"Structured {base_symbol}"

    if base_symbol and collateral_symbol and collateral_symbol != base_symbol:
        return f"{collateral_symbol} / {base_symbol}"
    if base_symbol:
        return f"{base_symbol} {protocol_code}"
    return market_address


def market_exposure_kind(
    *, market_kind_value: str, base_token_id: int | None, collateral_token_id: int | None
) -> str:
    if market_kind_value == "reserve":
        return "reserve_pair"
    if market_kind_value == "other":
        return "native_market"
    if collateral_token_id is not None and collateral_token_id != base_token_id:
        return "reserve_pair"
    if market_kind_value in {"vault", "vault_underlying"}:
        return "vault_exposure"
    return "native_market"


def market_exposure_tokens(
    *,
    market_kind_value: str,
    base_token_id: int | None,
    collateral_token_id: int | None,
) -> tuple[int | None, int | None, int | None]:
    exposure_kind = market_exposure_kind(
        market_kind_value=market_kind_value,
        base_token_id=base_token_id,
        collateral_token_id=collateral_token_id,
    )
    if exposure_kind == "reserve_pair":
        if collateral_token_id is not None and collateral_token_id != base_token_id:
            return collateral_token_id, base_token_id, collateral_token_id
        return base_token_id, base_token_id, None
    if exposure_kind == "vault_exposure":
        return base_token_id, None, None
    return base_token_id, None, collateral_token_id


def market_exposure_display_name(
    *,
    market_kind_value: str,
    supply_symbol: str | None,
    debt_symbol: str | None,
    market_display: str,
) -> str:
    if market_kind_value == "reserve":
        return supply_symbol or market_display
    if supply_symbol and debt_symbol and supply_symbol != debt_symbol:
        return f"{supply_symbol} / {debt_symbol}"
    if supply_symbol:
        return supply_symbol
    return market_display


def market_exposure_slug(
    *,
    protocol_code: str,
    chain_code: str,
    display_name: str,
) -> str:
    return slugify(f"{protocol_code}-{chain_code}-{display_name}")


def leverage_ratio(*, supply_usd: Decimal, equity_usd: Decimal) -> Decimal | None:
    if equity_usd <= ZERO:
        return None
    return supply_usd / equity_usd
