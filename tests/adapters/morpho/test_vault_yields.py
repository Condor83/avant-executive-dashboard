"""Morpho vault APY client tests."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from adapters.morpho.vault_yields import MorphoVaultYieldClient


def test_get_vault_apy_parses_base_and_rewards() -> None:
    observed_payloads: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed_payloads.append(request.content.decode())
        return httpx.Response(
            200,
            json={
                "data": {
                    "vaultV2ByAddress": {
                        "address": "0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
                        "avgNetApy": 0.10660328634479907,
                        "avgNetApyExcludingRewards": 0.10352315677551797,
                        "rewards": [
                            {
                                "asset": {"symbol": "MORPHO"},
                                "supplyApr": 0.003080129569281099,
                            }
                        ],
                    }
                }
            },
        )

    client = MorphoVaultYieldClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    try:
        quote = client.get_vault_apy(
            address="0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
            chain_id=1,
            lookback="SIX_HOURS",
        )
    finally:
        client.close()

    assert quote.net_apy == Decimal("0.10660328634479907")
    assert quote.base_apy_excluding_rewards == Decimal("0.10352315677551797")
    assert quote.reward_apy == Decimal("0.003080129569281099")
    assert quote.source == "morpho_api"
    assert quote.lookback == "SIX_HOURS"
    assert len(observed_payloads) == 1
    assert '"chainId":1' in observed_payloads[0]
    assert '"netApyLookback":"SIX_HOURS"' in observed_payloads[0]


def test_get_vault_apy_raises_when_vault_is_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, json={"data": {"vaultV2ByAddress": None}})

    client = MorphoVaultYieldClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    try:
        with pytest.raises(RuntimeError, match="Morpho vault not found"):
            client.get_vault_apy(
                address="0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
                chain_id=1,
            )
    finally:
        client.close()


def test_get_vault_apy_rejects_negative_rates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={
                "data": {
                    "vaultV2ByAddress": {
                        "address": "0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
                        "avgNetApy": -0.1,
                        "avgNetApyExcludingRewards": 0.01,
                        "rewards": [],
                    }
                }
            },
        )

    client = MorphoVaultYieldClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    try:
        with pytest.raises(RuntimeError, match="negative"):
            client.get_vault_apy(
                address="0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
                chain_id=1,
            )
    finally:
        client.close()
