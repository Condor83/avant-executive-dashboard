"""Yield helpers with in-process caching."""

from __future__ import annotations

from decimal import Decimal

import httpx

PCT_TO_UNIT = Decimal("100")
AVANT_APY_ENDPOINTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "SAVUSD": ("/savusdApy", ("savusdApy", "apy")),
    "SAVETH": ("/savethApy", ("apy", "savethApy")),
    "SAVBTC": ("/savbtcApy", ("savbtcApy", "apy")),
    "AVUSDX": ("/apy/avusdx", ("apy",)),
    "AVETHX": ("/apy/avethx", ("apy",)),
    "AVBTCX": ("/apy/avbtcx", ("apy",)),
}


class DefiLlamaYieldOracle:
    """Fetches DefiLlama pool APY values and caches by pool id."""

    def __init__(
        self,
        *,
        base_url: str = "https://yields.llama.fi",
        timeout_seconds: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._cache: dict[str, Decimal] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def get_pool_apy(self, pool_id: str) -> Decimal:
        """Return latest pool APY in 0.0-1.0 units from DefiLlama yields chart."""

        cached = self._cache.get(pool_id)
        if cached is not None:
            return cached

        response = self._client.get(f"{self.base_url}/chart/{pool_id}")
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") != "success":
            raise RuntimeError(f"unexpected DefiLlama response status: {payload}")

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("DefiLlama pool chart response has no rows")

        latest = rows[-1]
        apy_pct = latest.get("apy")
        if apy_pct is None:
            raise RuntimeError("DefiLlama pool chart row missing `apy`")

        apy = Decimal(str(apy_pct)) / PCT_TO_UNIT
        if apy < 0:
            raise RuntimeError(f"DefiLlama pool APY is negative for {pool_id}: {apy}")

        self._cache[pool_id] = apy
        return apy


class AvantYieldOracle:
    """Fetches Avant-native token APYs and caches by token symbol."""

    def __init__(
        self,
        *,
        base_url: str = "https://app.avantprotocol.com/api",
        timeout_seconds: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(
            timeout=timeout_seconds, follow_redirects=True
        )
        self._cache: dict[str, Decimal] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def get_token_apy(self, symbol: str) -> Decimal:
        """Return latest token APY in 0.0-1.0 units from Avant's API."""

        normalized = symbol.strip().upper()
        cached = self._cache.get(normalized)
        if cached is not None:
            return cached

        endpoint = AVANT_APY_ENDPOINTS.get(normalized)
        if endpoint is None:
            raise RuntimeError(f"Avant APY endpoint is not configured for {symbol}")

        path, candidate_keys = endpoint
        response = self._client.get(f"{self.base_url}{path}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Avant APY response for {symbol}: {payload!r}")

        apy_pct = None
        for key in candidate_keys:
            value = payload.get(key)
            if value is not None:
                apy_pct = value
                break
        if apy_pct is None:
            raise RuntimeError(f"Avant APY response missing APY field for {symbol}: {payload}")

        apy = Decimal(str(apy_pct)) / PCT_TO_UNIT
        if apy < 0:
            raise RuntimeError(f"Avant APY is negative for {symbol}: {apy}")

        self._cache[normalized] = apy
        return apy
