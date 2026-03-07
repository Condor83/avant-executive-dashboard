"""Bracket yield helpers."""

from __future__ import annotations

from decimal import Decimal

import httpx

BRACKET_NAV_ENDPOINTS: dict[str, tuple[str, int]] = {
    "WBRAVUSDC": ("bravUSDC", 30),
}
DAYS_PER_YEAR = Decimal("365")


class BracketNavYieldOracle:
    """Fetch current wrapper token APY from Bracket's public GraphQL API."""

    def __init__(
        self,
        *,
        graphql_url: str = "https://app.bracket.fi/api/vaults/graphql",
        timeout_seconds: float = 15.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.graphql_url = graphql_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._cache: dict[str, Decimal] = {}

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    @staticmethod
    def supports_token(symbol: str) -> bool:
        """Return whether the oracle knows how to price a token."""

        return symbol.strip().upper() in BRACKET_NAV_ENDPOINTS

    def get_token_apy(self, symbol: str) -> Decimal:
        """Return the current APY for a Bracket wrapper token."""

        normalized = symbol.strip().upper()
        cached = self._cache.get(normalized)
        if cached is not None:
            return cached

        endpoint = BRACKET_NAV_ENDPOINTS.get(normalized)
        if endpoint is None:
            raise RuntimeError(f"Bracket NAV endpoint is not configured for {symbol}")

        bracket_symbol, window_days = endpoint
        apy = self._fetch_current_apy(bracket_symbol)
        if apy is None:
            rows = self._fetch_nav_rows(bracket_symbol)
            if len(rows) < window_days:
                raise RuntimeError(
                    "Bracket NAV history is too short for "
                    f"{symbol}: need {window_days}, got {len(rows)}"
                )

            window = rows[-window_days:]
            start_nav = Decimal(str(window[0]["nav"]))
            end_nav = Decimal(str(window[-1]["onav"]))
            if start_nav <= 0 or end_nav <= 0:
                raise RuntimeError(
                    f"Bracket NAV values must be positive for {symbol}: "
                    f"start={start_nav} end={end_nav}"
                )

            apy = ((end_nav / start_nav) - Decimal("1")) * DAYS_PER_YEAR / Decimal(window_days)
        self._cache[normalized] = apy
        return apy

    def _fetch_current_apy(self, bracket_symbol: str) -> Decimal | None:
        response = self._client.post(
            self.graphql_url,
            json={
                "query": (
                    "query vaultsShortDetail{"
                    " vaults_short_detail {"
                    "   symbol"
                    "   apy_series { epoch apy }"
                    " }"
                    "}"
                ),
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://app.bracket.fi",
                "Referer": f"https://app.bracket.fi/invest/{bracket_symbol}",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Bracket response payload: {payload!r}")
        errors = payload.get("errors")
        if errors:
            raise RuntimeError(f"Bracket GraphQL returned errors: {errors}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"Bracket GraphQL response missing data: {payload!r}")
        details = data.get("vaults_short_detail")
        if not isinstance(details, list):
            raise RuntimeError(f"Bracket short detail missing for {bracket_symbol}: {payload!r}")

        for detail in details:
            if not isinstance(detail, dict):
                continue
            if str(detail.get("symbol")) != bracket_symbol:
                continue
            apy_series = detail.get("apy_series")
            if not isinstance(apy_series, list):
                return None
            for entry in reversed(apy_series):
                if not isinstance(entry, dict):
                    continue
                apy_raw = entry.get("apy")
                if apy_raw is None:
                    continue
                apy = Decimal(str(apy_raw)) / Decimal("100")
                if apy > Decimal("0"):
                    return apy
            return None
        return None

    def _fetch_nav_rows(self, bracket_symbol: str) -> list[dict[str, object]]:
        response = self._client.post(
            self.graphql_url,
            json={
                "query": (
                    "query vault($symbol: String){"
                    " vault(symbol:$symbol){"
                    "   navs { epoch nav onav epoch_duration_days created updated }"
                    " }"
                    "}"
                ),
                "variables": {"symbol": bracket_symbol},
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://app.bracket.fi",
                "Referer": f"https://app.bracket.fi/invest/{bracket_symbol}",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Bracket response payload: {payload!r}")
        errors = payload.get("errors")
        if errors:
            raise RuntimeError(f"Bracket GraphQL returned errors: {errors}")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"Bracket GraphQL response missing data: {payload!r}")
        vault = data.get("vault")
        if not isinstance(vault, dict):
            raise RuntimeError(f"Bracket vault missing for symbol {bracket_symbol}: {payload!r}")
        navs = vault.get("navs")
        if not isinstance(navs, list):
            raise RuntimeError(f"Bracket NAV history missing for {bracket_symbol}: {payload!r}")
        return [row for row in navs if isinstance(row, dict)]
