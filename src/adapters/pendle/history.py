"""Pendle market metadata, wallet positions, and trade history helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import httpx

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class PendleTrade:
    """Normalized Pendle trade row used for PT fixed-yield reconstruction."""

    market_address: str
    action: str
    timestamp: datetime
    implied_apy: Decimal
    pt_notional: Decimal


@dataclass(frozen=True)
class PendleMarketMetadata:
    """Normalized Pendle market metadata used by the adapter and PT history reads."""

    chain_id: int
    market_address: str
    name: str | None
    expiry: datetime | None
    pt_token_address: str | None
    yt_token_address: str | None
    sy_token_address: str | None
    underlying_token_address: str | None
    liquidity_usd: Decimal | None
    total_tvl_usd: Decimal | None
    total_pt: Decimal | None
    total_sy: Decimal | None
    underlying_apy: Decimal | None
    implied_apy: Decimal | None
    pendle_apy: Decimal | None
    swap_fee_apy: Decimal | None
    aggregated_apy: Decimal | None


@dataclass(frozen=True)
class PendleWalletPosition:
    """Normalized PT/YT wallet balances for a single Pendle market."""

    chain_id: int
    market_address: str
    pt_balance: Decimal
    pt_valuation_usd: Decimal
    yt_balance: Decimal
    yt_valuation_usd: Decimal


class PendleHistoryClient:
    """Fetch Pendle market metadata, wallet positions, and wallet transaction history."""

    def __init__(
        self,
        *,
        base_url: str = "https://api-v2.pendle.finance/core",
        timeout_seconds: float = 15.0,
        max_attempts: int = 3,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._markets_by_chain: dict[int, list[PendleMarketMetadata]] = {}
        self._market_cache: dict[int, dict[str, set[str]]] = {}
        self._trade_cache: dict[tuple[int, str], list[PendleTrade]] = {}
        self._position_cache: dict[tuple[int, str], list[PendleWalletPosition]] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def _get_json(
        self,
        path: str,
        *,
        params: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> object:
        last_error: Exception | None = None
        for _attempt in range(1, self.max_attempts + 1):
            try:
                response = self._client.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers={"Accept": "application/json"},
                )
                if response.status_code in RETRYABLE_STATUS_CODES:
                    last_error = httpx.HTTPStatusError(
                        "retryable status",
                        request=response.request,
                        response=response,
                    )
                    continue
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as exc:
                last_error = exc
                continue
        if last_error is None:
            raise RuntimeError(f"Pendle request failed path={path}")
        raise last_error

    def get_markets(self, *, chain_id: int) -> list[PendleMarketMetadata]:
        """Return normalized Pendle markets for one chain."""

        cached = self._markets_by_chain.get(chain_id)
        if cached is not None:
            return list(cached)

        payload = self._get_json("/v1/markets/all", params={"chainId": chain_id})
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Pendle market list payload: {payload!r}")
        markets = payload.get("markets")
        if not isinstance(markets, list):
            raise RuntimeError(f"Pendle market list missing `markets`: {payload!r}")

        parsed: list[PendleMarketMetadata] = []
        chain_cache: dict[str, set[str]] = {}
        for row in markets:
            market = self._parse_market(row)
            if market is None:
                continue
            parsed.append(market)
            if market.pt_token_address:
                chain_cache.setdefault(market.pt_token_address, set()).add(market.market_address)

        self._markets_by_chain[chain_id] = parsed
        self._market_cache[chain_id] = chain_cache
        return list(parsed)

    def get_market_addresses_for_pt(self, *, chain_id: int, pt_token_address: str) -> set[str]:
        """Return Pendle market addresses that trade the given PT token."""

        normalized_pt = pt_token_address.strip().lower()
        chain_cache = self._market_cache.get(chain_id)
        if chain_cache is None:
            self.get_markets(chain_id=chain_id)
            chain_cache = self._market_cache.get(chain_id, {})
        return set(chain_cache.get(normalized_pt, set()))

    def get_wallet_positions(
        self, *, chain_id: int, wallet_address: str
    ) -> list[PendleWalletPosition]:
        """Return normalized Pendle PT/YT wallet balances for one chain."""

        cache_key = (chain_id, wallet_address.strip().lower())
        cached = self._position_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        payload = self._get_json(
            f"/v1/dashboard/positions/database/{wallet_address}",
            params={"filterUsd": 1},
        )
        rows = self._extract_market_position_rows(payload)
        aggregated: dict[str, PendleWalletPosition] = {}
        for row in rows:
            position = self._parse_wallet_position(row)
            if position is None or position.chain_id != chain_id:
                continue
            existing = aggregated.get(position.market_address)
            if existing is None:
                aggregated[position.market_address] = position
                continue
            aggregated[position.market_address] = PendleWalletPosition(
                chain_id=chain_id,
                market_address=position.market_address,
                pt_balance=existing.pt_balance + position.pt_balance,
                pt_valuation_usd=existing.pt_valuation_usd + position.pt_valuation_usd,
                yt_balance=existing.yt_balance + position.yt_balance,
                yt_valuation_usd=existing.yt_valuation_usd + position.yt_valuation_usd,
            )

        positions = list(aggregated.values())
        self._position_cache[cache_key] = positions
        return list(positions)

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
            payload = self._get_json(f"/v5/{chain_id}/transactions/{wallet_address}", params=params)
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

    @staticmethod
    def _parse_decimal(value: object) -> Decimal | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _asset_address(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        _, _, address = value.partition("-")
        candidate = address or value
        candidate = candidate.strip().lower()
        return candidate or None

    def _parse_market(self, row: object) -> PendleMarketMetadata | None:
        if not isinstance(row, Mapping):
            return None
        market_address = row.get("address")
        chain_id = row.get("chainId")
        if not isinstance(market_address, str) or chain_id is None:
            return None
        details_obj = row.get("details")
        details: Mapping[str, object] = details_obj if isinstance(details_obj, Mapping) else {}
        return PendleMarketMetadata(
            chain_id=int(chain_id),
            market_address=market_address.strip().lower(),
            name=row.get("name") if isinstance(row.get("name"), str) else None,
            expiry=self._parse_optional_timestamp(row.get("expiry")),
            pt_token_address=self._asset_address(row.get("pt")),
            yt_token_address=self._asset_address(row.get("yt")),
            sy_token_address=self._asset_address(row.get("sy")),
            underlying_token_address=self._asset_address(row.get("underlyingAsset")),
            liquidity_usd=self._parse_decimal(details.get("liquidity")),
            total_tvl_usd=self._parse_decimal(details.get("totalTvl")),
            total_pt=self._parse_decimal(details.get("totalPt")),
            total_sy=self._parse_decimal(details.get("totalSy")),
            underlying_apy=self._parse_decimal(details.get("underlyingApy")),
            implied_apy=self._parse_decimal(details.get("impliedApy")),
            pendle_apy=self._parse_decimal(details.get("pendleApy")),
            swap_fee_apy=self._parse_decimal(details.get("swapFeeApy")),
            aggregated_apy=self._parse_decimal(details.get("aggregatedApy")),
        )

    def _extract_market_position_rows(self, payload: object) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []

        def walk(node: object) -> None:
            if isinstance(node, dict):
                if isinstance(node.get("marketId"), str) and (
                    isinstance(node.get("pt"), dict) or isinstance(node.get("yt"), dict)
                ):
                    rows.append(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(payload)
        return rows

    def _parse_wallet_position(self, row: Mapping[str, object]) -> PendleWalletPosition | None:
        market_id = row.get("marketId")
        if not isinstance(market_id, str):
            return None
        chain_prefix, _, market_address = market_id.partition("-")
        if not market_address:
            return None

        pt_obj = row.get("pt")
        yt_obj = row.get("yt")
        pt: Mapping[str, object] = pt_obj if isinstance(pt_obj, Mapping) else {}
        yt: Mapping[str, object] = yt_obj if isinstance(yt_obj, Mapping) else {}
        pt_balance = self._parse_decimal(pt.get("balance")) or Decimal("0")
        pt_valuation = self._parse_decimal(pt.get("valuation")) or Decimal("0")
        yt_balance = self._parse_decimal(yt.get("balance")) or Decimal("0")
        yt_valuation = self._parse_decimal(yt.get("valuation")) or Decimal("0")
        return PendleWalletPosition(
            chain_id=int(chain_prefix),
            market_address=market_address.strip().lower(),
            pt_balance=pt_balance,
            pt_valuation_usd=pt_valuation,
            yt_balance=yt_balance,
            yt_valuation_usd=yt_valuation,
        )

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

    @classmethod
    def _parse_optional_timestamp(cls, value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        stripped = value.strip()
        if not stripped:
            return None
        candidate = stripped if "T" in stripped else f"{stripped}T00:00:00Z"
        return cls._parse_timestamp(candidate)
