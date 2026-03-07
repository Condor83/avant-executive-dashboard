"""Morpho vault APY lookups backed by Morpho's official GraphQL API."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

ZERO = Decimal("0")

_QUERY = """
query GetVaultV2ApyOverview(
  $address: String!
  $chainId: Int!
  $netApyLookback: VaultV2LookbackPeriod
) {
  vaultV2ByAddress(address: $address, chainId: $chainId) {
    address
    avgNetApy(lookback: $netApyLookback)
    avgNetApyExcludingRewards
    rewards {
      asset {
        symbol
      }
      supplyApr
    }
  }
}
"""


@dataclass(frozen=True)
class MorphoVaultApyQuote:
    """Normalized Morpho vault APY split in 0.0-1.0 units."""

    net_apy: Decimal
    base_apy_excluding_rewards: Decimal
    reward_apy: Decimal
    lookback: str
    source: str = "morpho_api"


class MorphoVaultYieldClient:
    """Fetch current Morpho vault APY from Morpho's official GraphQL endpoint."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.morpho.org/graphql",
        timeout_seconds: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._cache: dict[tuple[str, int, str], MorphoVaultApyQuote] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def get_vault_apy(
        self,
        *,
        address: str,
        chain_id: int,
        lookback: str = "SIX_HOURS",
    ) -> MorphoVaultApyQuote:
        """Return Morpho vault APY split for the requested vault and lookback."""

        cache_key = (address.lower(), chain_id, lookback)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        response = self._client.post(
            self.base_url,
            json={
                "query": _QUERY,
                "variables": {
                    "address": address,
                    "chainId": chain_id,
                    "netApyLookback": lookback,
                },
            },
        )
        response.raise_for_status()
        payload = response.json()

        errors = payload.get("errors")
        if errors:
            raise RuntimeError(f"Morpho GraphQL returned errors: {errors}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Morpho GraphQL response missing `data`")
        vault = data.get("vaultV2ByAddress")
        if not isinstance(vault, dict):
            raise RuntimeError(f"Morpho vault not found for address={address} chain_id={chain_id}")

        net_apy = _parse_decimal(vault.get("avgNetApy"), field_name="avgNetApy")
        base_apy = _parse_decimal(
            vault.get("avgNetApyExcludingRewards"),
            field_name="avgNetApyExcludingRewards",
        )
        reward_apy = _sum_reward_apr(vault.get("rewards"))

        if net_apy < ZERO:
            raise RuntimeError(f"Morpho vault avgNetApy is negative for {address}: {net_apy}")
        if base_apy < ZERO:
            raise RuntimeError(
                f"Morpho vault avgNetApyExcludingRewards is negative for {address}: {base_apy}"
            )

        quote = MorphoVaultApyQuote(
            net_apy=net_apy,
            base_apy_excluding_rewards=base_apy,
            reward_apy=reward_apy,
            lookback=lookback,
        )
        self._cache[cache_key] = quote
        return quote


def _parse_decimal(value: Any, *, field_name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise RuntimeError(f"Morpho vault response missing numeric `{field_name}`")
    decimal_value = Decimal(str(value))
    if decimal_value < ZERO:
        raise RuntimeError(f"Morpho vault `{field_name}` is negative: {decimal_value}")
    return decimal_value


def _sum_reward_apr(value: Any) -> Decimal:
    if value is None:
        return ZERO
    if not isinstance(value, list):
        raise RuntimeError("Morpho vault response `rewards` is not a list")
    total = ZERO
    for entry in value:
        if not isinstance(entry, dict):
            raise RuntimeError("Morpho vault reward entry is not an object")
        total += _parse_decimal(entry.get("supplyApr", ZERO), field_name="rewards[].supplyApr")
    return total
