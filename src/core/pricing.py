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

# Targeted pricing controls for tokens missing direct DefiLlama coverage.
MANUAL_PRICE_OVERRIDES_USD: dict[tuple[str, str], Decimal] = {
    # Linea avUSD
    ("linea", "0x37c44fc08e403efc0946c0623cb1164a52ce1576"): Decimal("1"),
}

PRICE_ALIAS_TARGETS: dict[tuple[str, str], tuple[str, str]] = {
    # Linea savUSD -> Avalanche savUSD
    (
        "linea",
        "0x5c247948fd58bb02b6c4678d9940f5e6b9af1127",
    ): (
        "avalanche",
        "0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
    ),
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

        request_by_coin: dict[str, list[tuple[PriceRequest, tuple[str, str] | None]]] = {}
        for request in requests:
            normalized_address = self._normalize_address(request.address_or_mint)
            token_key = (request.chain_code, normalized_address)

            overridden_price = MANUAL_PRICE_OVERRIDES_USD.get(token_key)
            if overridden_price is not None:
                quotes.append(
                    PriceQuote(
                        token_id=request.token_id,
                        chain_code=request.chain_code,
                        address_or_mint=normalized_address,
                        price_usd=overridden_price,
                    )
                )
                continue

            alias_target = PRICE_ALIAS_TARGETS.get(token_key)
            if alias_target is not None:
                alias_chain_code, alias_address = alias_target
                coin_id = self.coin_id_for(alias_chain_code, alias_address)
            else:
                coin_id = self.coin_id_for(request.chain_code, request.address_or_mint)

            if coin_id is None:
                payload_json: dict[str, object] = {
                    "token_id": request.token_id,
                    "symbol": request.symbol,
                }
                if alias_target is not None:
                    payload_json["alias_target_chain"] = alias_target[0]
                    payload_json["alias_target_address"] = alias_target[1]
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
                        payload_json=payload_json,
                    )
                )
                continue
            request_by_coin.setdefault(coin_id, []).append((request, alias_target))

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
                    for request, alias_target in request_by_coin[coin_id]:
                        payload_json = {
                            "token_id": request.token_id,
                            "symbol": request.symbol,
                        }
                        if alias_target is not None:
                            payload_json["alias_target_chain"] = alias_target[0]
                            payload_json["alias_target_address"] = alias_target[1]
                        issues.append(
                            DataQualityIssue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_prices",
                                error_type="price_fetch_failed",
                                error_message=str(exc),
                                chain_code=request.chain_code,
                                market_ref=request.address_or_mint,
                                payload_json=payload_json,
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
                for request, alias_target in requests_for_coin:
                    payload_json = {
                        "token_id": request.token_id,
                        "symbol": request.symbol,
                    }
                    if alias_target is not None:
                        payload_json["alias_target_chain"] = alias_target[0]
                        payload_json["alias_target_address"] = alias_target[1]
                    issues.append(
                        DataQualityIssue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_prices",
                            error_type="price_missing",
                            error_message="DefiLlama response did not include a price for token",
                            chain_code=request.chain_code,
                            market_ref=request.address_or_mint,
                            payload_json=payload_json,
                        )
                    )
                continue

            for request, _alias_target in requests_for_coin:
                quotes.append(
                    PriceQuote(
                        token_id=request.token_id,
                        chain_code=request.chain_code,
                        address_or_mint=self._normalize_address(request.address_or_mint),
                        price_usd=price_usd,
                    )
                )

        return PriceFetchResult(quotes=quotes, issues=issues)
