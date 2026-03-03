"""DefiLlama-backed yield helpers with in-process caching."""

from __future__ import annotations

from decimal import Decimal

import httpx

PCT_TO_UNIT = Decimal("100")


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
