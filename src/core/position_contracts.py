"""Shared helpers for interpreting canonical position snapshot fields."""

from __future__ import annotations

from decimal import Decimal

ZERO = Decimal("0")


def uses_collateral_as_supply(
    *,
    collateral_token_id: int | None,
    collateral_amount: Decimal | None,
    collateral_usd: Decimal | None,
) -> bool:
    if collateral_token_id is None:
        return False
    if collateral_amount is not None and collateral_amount > ZERO:
        return True
    if collateral_usd is not None and collateral_usd > ZERO:
        return True
    return False


def economic_supply_amount(
    *,
    supplied_amount: Decimal,
    collateral_amount: Decimal | None,
    collateral_token_id: int | None,
    collateral_usd: Decimal | None,
) -> Decimal:
    if uses_collateral_as_supply(
        collateral_token_id=collateral_token_id,
        collateral_amount=collateral_amount,
        collateral_usd=collateral_usd,
    ):
        return collateral_amount or ZERO
    return supplied_amount


def economic_supply_usd(
    *,
    supplied_usd: Decimal,
    collateral_usd: Decimal | None,
    collateral_token_id: int | None,
    collateral_amount: Decimal | None,
) -> Decimal:
    if uses_collateral_as_supply(
        collateral_token_id=collateral_token_id,
        collateral_amount=collateral_amount,
        collateral_usd=collateral_usd,
    ):
        return collateral_usd or ZERO
    return supplied_usd


def economic_supply_token_id(
    *,
    base_asset_token_id: int | None,
    collateral_token_id: int | None,
    collateral_amount: Decimal | None,
    collateral_usd: Decimal | None,
) -> int | None:
    if uses_collateral_as_supply(
        collateral_token_id=collateral_token_id,
        collateral_amount=collateral_amount,
        collateral_usd=collateral_usd,
    ):
        return collateral_token_id
    return base_asset_token_id or collateral_token_id
