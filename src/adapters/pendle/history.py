"""Pendle market metadata and wallet trade history helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import httpx


@dataclass(frozen=True)
class PendleTrade:
    """Normalized Pendle trade row used for PT fixed-yield reconstruction."""

    market_address: str
    action: str
    timestamp: datetime
    implied_apy: Decimal
    pt_notional: Decimal


class PendleHistoryClient:
    """Fetch Pendle market metadata and wallet transaction history."""

    def __init__(
        self,
        *,
        base_url: str = "https://api-v2.pendle.finance/core",
        timeout_seconds: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._market_cache: dict[int, dict[str, set[str]]] = {}
        self._trade_cache: dict[tuple[int, str], list[PendleTrade]] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def get_market_addresses_for_pt(self, *, chain_id: int, pt_token_address: str) -> set[str]:
        """Return Pendle market addresses that trade the given PT token."""

        normalized_pt = pt_token_address.strip().lower()
        chain_cache = self._market_cache.get(chain_id)
        if chain_cache is None:
            response = self._client.get(
                f"{self.base_url}/v1/markets/all",
                params={"chainId": chain_id},
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"unexpected Pendle market list payload: {payload!r}")
            markets = payload.get("markets")
            if not isinstance(markets, list):
                raise RuntimeError(f"Pendle market list missing `markets`: {payload!r}")
            chain_cache = {}
            for row in markets:
                if not isinstance(row, dict):
                    continue
                market_address = row.get("address")
                pt_id = row.get("pt")
                if not isinstance(market_address, str) or not isinstance(pt_id, str):
                    continue
                _, _, pt_address = pt_id.partition("-")
                if not pt_address:
                    continue
                chain_cache.setdefault(pt_address.strip().lower(), set()).add(
                    market_address.strip().lower()
                )
            self._market_cache[chain_id] = chain_cache
        return set(chain_cache.get(normalized_pt, set()))

    def get_wallet_trades(self, *, chain_id: int, wallet_address: str) -> list[PendleTrade]:
        """Return normalized Pendle trade history for a wallet."""

        cache_key = (chain_id, wallet_address.strip().lower())
        cached = self._trade_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        trades: list[PendleTrade] = []
        resume_token: str | None = None
        while True:
            params: dict[str, str | int] = {"type": "TRADES", "limit": 1000}
            if resume_token:
                params["resumeToken"] = resume_token
            response = self._client.get(
                f"{self.base_url}/v5/{chain_id}/transactions/{wallet_address}",
                params=params,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"unexpected Pendle transaction payload: {payload!r}")
            results = payload.get("results")
            if not isinstance(results, list):
                raise RuntimeError(f"Pendle transaction payload missing `results`: {payload!r}")
            trades.extend(self._parse_trades(results))
            resume_token = payload.get("resumeToken")
            if not isinstance(resume_token, str) or not resume_token:
                break

        trades.sort(key=lambda trade: trade.timestamp)
        self._trade_cache[cache_key] = trades
        return list(trades)

    def _parse_trades(self, rows: list[object]) -> list[PendleTrade]:
        parsed: list[PendleTrade] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            market_address = row.get("market")
            action = row.get("action")
            timestamp = row.get("timestamp")
            implied_apy = row.get("impliedApy")
            notional = row.get("notional")
            pt_notional = notional.get("pt") if isinstance(notional, dict) else None
            if not isinstance(market_address, str) or not isinstance(action, str):
                continue
            if not isinstance(timestamp, str) or implied_apy is None or pt_notional is None:
                continue
            parsed.append(
                PendleTrade(
                    market_address=market_address.strip().lower(),
                    action=action.strip().upper(),
                    timestamp=self._parse_timestamp(timestamp),
                    implied_apy=Decimal(str(implied_apy)),
                    pt_notional=Decimal(str(pt_notional)),
                )
            )
        return parsed

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
