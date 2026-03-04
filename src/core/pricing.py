"""DefiLlama-backed token pricing service with in-process caching."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx

from core.types import DataQualityIssue, PriceQuote, PriceRequest

CHAIN_TO_DEFILLAMA: dict[str, str] = {
    "arbitrum": "arbitrum",
    "avalanche": "avax",
    "base": "base",
    "bera": "berachain",
    "ethereum": "ethereum",
    "ink": "ink",
    "linea": "linea",
    "mantle": "mantle",
    "plasma": "plasma",
    "solana": "solana",
    "stacks": "stacks",
    "sonic": "sonic",
}


@dataclass(frozen=True)
class PriceFetchResult:
    """Result of a price fetch operation."""

    quotes: list[PriceQuote]
    issues: list[DataQualityIssue]


class PriceOracle:
    """Fetches token prices from DefiLlama and caches coin lookups."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=self.timeout_seconds)
        self._cache: dict[str, Decimal] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    @staticmethod
    def _normalize_address(address: str) -> str:
        address = address.strip()
        if address.startswith("0x"):
            return address.lower()
        return address

    @classmethod
    def coin_id_for(cls, chain_code: str, address_or_mint: str) -> str | None:
        """Return a DefiLlama coin identifier for supported chains."""

        chain = CHAIN_TO_DEFILLAMA.get(chain_code)
        if not chain:
            return None

        normalized_address = cls._normalize_address(address_or_mint)
        if chain_code in {"solana", "stacks"}:
            if not normalized_address:
                return None
            return f"{chain}:{normalized_address}"
        if not normalized_address.startswith("0x"):
            return None

        return f"{chain}:{normalized_address}"

    @staticmethod
    def _chunk(values: list[str], size: int) -> Iterable[list[str]]:
        for start in range(0, len(values), size):
            yield values[start : start + size]

    def fetch_prices(
        self,
        requests: list[PriceRequest],
        *,
        as_of_ts_utc: datetime,
    ) -> PriceFetchResult:
        """Fetch prices for requests and return quotes plus data-quality issues."""

        issues: list[DataQualityIssue] = []
        quotes: list[PriceQuote] = []

        request_by_coin: dict[str, list[PriceRequest]] = {}
        for request in requests:
            coin_id = self.coin_id_for(request.chain_code, request.address_or_mint)
            if coin_id is None:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_prices",
                        error_type="unsupported_chain_or_address",
                        error_message=(
                            "token not supported by DefiLlama price endpoint for current mapping"
                        ),
                        protocol_code=None,
                        chain_code=request.chain_code,
                        market_ref=request.address_or_mint,
                        payload_json={"token_id": request.token_id, "symbol": request.symbol},
                    )
                )
                continue
            request_by_coin.setdefault(coin_id, []).append(request)

        if not request_by_coin:
            return PriceFetchResult(quotes=quotes, issues=issues)

        unresolved_coin_ids = [coin_id for coin_id in request_by_coin if coin_id not in self._cache]

        for chunk in self._chunk(unresolved_coin_ids, 50):
            endpoint = f"{self.base_url}/prices/current/{','.join(chunk)}"
            try:
                response = self._client.get(endpoint)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:  # pragma: no cover - network failure path
                for coin_id in chunk:
                    for request in request_by_coin[coin_id]:
                        issues.append(
                            DataQualityIssue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_prices",
                                error_type="price_fetch_failed",
                                error_message=str(exc),
                                chain_code=request.chain_code,
                                market_ref=request.address_or_mint,
                                payload_json={
                                    "token_id": request.token_id,
                                    "symbol": request.symbol,
                                },
                            )
                        )
                continue

            coins_payload = payload.get("coins", {}) if isinstance(payload, dict) else {}
            for coin_id in chunk:
                price_payload = coins_payload.get(coin_id)
                if not price_payload or "price" not in price_payload:
                    continue
                self._cache[coin_id] = Decimal(str(price_payload["price"]))

        for coin_id, requests_for_coin in request_by_coin.items():
            price_usd = self._cache.get(coin_id)
            if price_usd is None:
                for request in requests_for_coin:
                    issues.append(
                        DataQualityIssue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_prices",
                            error_type="price_missing",
                            error_message="DefiLlama response did not include a price for token",
                            chain_code=request.chain_code,
                            market_ref=request.address_or_mint,
                            payload_json={"token_id": request.token_id, "symbol": request.symbol},
                        )
                    )
                continue

            for request in requests_for_coin:
                quotes.append(
                    PriceQuote(
                        token_id=request.token_id,
                        chain_code=request.chain_code,
                        address_or_mint=self._normalize_address(request.address_or_mint),
                        price_usd=price_usd,
                    )
                )

        return PriceFetchResult(quotes=quotes, issues=issues)
