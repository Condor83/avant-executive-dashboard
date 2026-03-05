"""Euler config parsing tests."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from core.config import MarketsConfig


def _base_payload() -> dict[str, Any]:
    return {
        "aave_v3": {},
        "spark": {},
        "morpho": {},
        "euler_v2": {
            "avalanche": {
                "wallets": ["0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd"],
                "vaults": [
                    {
                        "address": "0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e",
                        "symbol": "eUSDC",
                        "asset_address": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                        "asset_symbol": "USDC",
                        "asset_decimals": 6,
                    }
                ],
            }
        },
        "dolomite": {},
        "kamino": {},
        "zest": {},
        "wallet_balances": {},
    }


def test_euler_account_ids_default_to_zero() -> None:
    parsed = MarketsConfig.model_validate(_base_payload())
    assert parsed.euler_v2["avalanche"].account_ids == [0]
    assert parsed.euler_v2["avalanche"].vaults[0].debt_supported is True


def test_euler_account_ids_reject_negative_values() -> None:
    payload = _base_payload()
    payload["euler_v2"]["avalanche"]["account_ids"] = [0, -1]

    with pytest.raises(ValidationError):
        MarketsConfig.model_validate(payload)


def test_euler_vault_debt_supported_can_be_disabled() -> None:
    payload = _base_payload()
    payload["euler_v2"]["avalanche"]["vaults"][0]["debt_supported"] = False
    parsed = MarketsConfig.model_validate(payload)
    assert parsed.euler_v2["avalanche"].vaults[0].debt_supported is False
