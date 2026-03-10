"""DeBank Cloud API client helpers."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class DebankResponseError(RuntimeError):
    """Error raised for DeBank API request failures."""

    message: str
    status_code: int | None = None
    payload: str | None = None

    def __str__(self) -> str:
        if self.status_code is None:
            return self.message
        if self.payload:
            return f"{self.message} status={self.status_code} payload={self.payload}"
        return f"{self.message} status={self.status_code}"


class DebankCloudClient:
    """Minimal DeBank Cloud client for wallet-level protocol exposure queries."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout_seconds: float = 15.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        client: httpx.Client | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[], float] = random.random,
    ) -> None:
        normalized_key = api_key.strip()
        if not normalized_key:
            raise ValueError("debank api key must be non-empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds must be >= 0")

        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self._sleep_fn = sleep_fn
        self._jitter_fn = jitter_fn
        self._headers = {"AccessKey": normalized_key}
        self._client = client or httpx.Client(timeout=self.timeout_seconds)

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def _backoff_sleep_seconds(self, attempt_number: int) -> float:
        # Exponential backoff with small jitter to avoid synchronized retries.
        return self.backoff_seconds * (2 ** (attempt_number - 1)) + (self._jitter_fn() * 0.1)

    def _get_json(self, path: str, *, params: dict[str, str] | None = None) -> Any:
        last_error: DebankResponseError | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self._client.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers=self._headers,
                )
            except httpx.HTTPError as exc:
                last_error = DebankResponseError(
                    message=f"debank request failed for path={path}: {exc}",
                )
                if attempt < self.max_attempts:
                    self._sleep_fn(self._backoff_sleep_seconds(attempt))
                    continue
                break

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_attempts:
                self._sleep_fn(self._backoff_sleep_seconds(attempt))
                continue

            if response.status_code >= 400:
                last_error = DebankResponseError(
                    message=f"debank request returned error for path={path}",
                    status_code=response.status_code,
                    payload=response.text,
                )
                break

            return response.json()

        if last_error is None:
            raise DebankResponseError(message=f"debank request failed for path={path}")
        raise last_error

    def get_user_complex_protocols(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        """Return all complex protocol positions for a wallet."""

        params: dict[str, str] = {"id": wallet_address}
        if chain_ids:
            normalized_chain_ids = [chain_id.strip() for chain_id in chain_ids if chain_id.strip()]
            if normalized_chain_ids:
                params["chain_ids"] = ",".join(normalized_chain_ids)

        payload = self._get_json("/v1/user/all_complex_protocol_list", params=params)
        if not isinstance(payload, list):
            raise DebankResponseError(
                f"debank wallet payload is not a list for wallet={wallet_address}"
            )

        result: list[dict[str, object]] = []
        for item in payload:
            if isinstance(item, dict):
                result.append(item)
        return result

    def get_user_all_tokens(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
        include_protocol_tokens: bool = True,
    ) -> list[dict[str, object]]:
        """Return wallet token balances for a wallet across supported chains."""

        params: dict[str, str] = {"id": wallet_address}
        if include_protocol_tokens:
            params["is_all"] = "true"
        if chain_ids:
            normalized_chain_ids = [chain_id.strip() for chain_id in chain_ids if chain_id.strip()]
            if normalized_chain_ids:
                params["chain_ids"] = ",".join(normalized_chain_ids)

        payload = self._get_json("/v1/user/all_token_list", params=params)
        if not isinstance(payload, list):
            raise DebankResponseError(
                f"debank token payload is not a list for wallet={wallet_address}"
            )

        result: list[dict[str, object]] = []
        for item in payload:
            if isinstance(item, dict):
                result.append(item)
        return result
