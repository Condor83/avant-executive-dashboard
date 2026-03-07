"""Dolomite config-specific parsing tests."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from core.config import MarketsConfig


def _base_payload() -> dict[str, Any]:
    return {
        "aave_v3": {},
        "morpho": {},
        "euler_v2": {},
        "dolomite": {
            "bera": {
                "margin": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
                "wallets": ["0xc1d023141ad6935f81e5286e577768b75c9ff8eb"],
                "markets": [
                    {
                        "id": 2,
                        "symbol": "USDC.e",
                        "token_address": "0x549943e04f40284185054145c6e4e9568c1d3241",
                        "defillama_pool_id": "pool-1",
                        "decimals": 6,
                    }
                ],
            }
        },
        "kamino": {},
        "zest": {},
        "wallet_balances": {},
    }


def test_dolomite_account_numbers_default_to_zero() -> None:
    parsed = MarketsConfig.model_validate(_base_payload())
    assert parsed.dolomite["bera"].account_numbers == [0]


def test_dolomite_account_numbers_reject_negative_values() -> None:
    payload = _base_payload()
    payload["dolomite"]["bera"]["account_numbers"] = [0, -1]

    with pytest.raises(ValidationError):
        MarketsConfig.model_validate(payload)


def test_dolomite_market_token_address_is_optional() -> None:
    parsed = MarketsConfig.model_validate(_base_payload())
    assert parsed.dolomite["bera"].markets[0].token_address == (
        "0x549943e04f40284185054145c6e4e9568c1d3241"
    )
    assert parsed.dolomite["bera"].markets[0].defillama_pool_id == "pool-1"

    payload = _base_payload()
    del payload["dolomite"]["bera"]["markets"][0]["token_address"]
    parsed_without_address = MarketsConfig.model_validate(payload)
    assert parsed_without_address.dolomite["bera"].markets[0].token_address is None
