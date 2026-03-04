"""Trader Joe Liquidity Book adapter for buy-wall ops positions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import MarketsConfig, TraderJoePool, canonical_address
from core.types import DataQualityIssue, PositionSnapshotInput

LB_GET_TOKEN_X_SELECTOR = "0x05e8746d"
LB_GET_TOKEN_Y_SELECTOR = "0xda10610c"
LB_GET_BIN_SELECTOR = "0x0abe9688"
LB_BALANCE_OF_SELECTOR = "0x00fdd58e"  # balanceOf(address account, uint256 id)
LB_TOTAL_SUPPLY_SELECTOR = "0xbd85b039"  # totalSupply(uint256 id)


def normalize_raw_amount(raw_amount: Decimal | int, decimals: int) -> Decimal:
    """Convert raw on-chain integer amounts into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _strip_0x_hex(value: str) -> str:
    cleaned = value.strip().lower()
    return cleaned[2:] if cleaned.startswith("0x") else cleaned


def _encode_address(value: str) -> str:
    return _strip_0x_hex(value).rjust(64, "0")


def _encode_uint(value: int) -> str:
    return hex(value)[2:].rjust(64, "0")


def _decode_words(raw_hex: str) -> list[int]:
    payload = _strip_0x_hex(raw_hex)
    if not payload:
        return []
    if len(payload) % 64 != 0:
        raise ValueError(f"invalid ABI payload length: {len(payload)}")
    words: list[int] = []
    for idx in range(0, len(payload), 64):
        words.append(int(payload[idx : idx + 64], 16))
    return words


def _decode_address_word(word: int) -> str:
    return f"0x{word.to_bytes(32, 'big')[-20:].hex()}"


class TraderJoeLpRpcClient(Protocol):
    """Protocol for Trader Joe LB reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_token_x(self, chain_code: str, pool_address: str) -> str:
        """Return the token X address for an LB pool."""

    def get_token_y(self, chain_code: str, pool_address: str) -> str:
        """Return the token Y address for an LB pool."""

    def get_bin(self, chain_code: str, pool_address: str, bin_id: int) -> tuple[int, int]:
        """Return `(reserve_x_raw, reserve_y_raw)` for a bin."""

    def get_bin_total_supply(self, chain_code: str, pool_address: str, bin_id: int) -> int:
        """Return total LB shares supply for a bin id."""

    def get_bin_balance(
        self, chain_code: str, pool_address: str, wallet_address: str, bin_id: int
    ) -> int:
        """Return wallet LB share balance for a bin id."""


class EvmRpcTraderJoeLpClient:
    """JSON-RPC client for Trader Joe LB pool calls."""

    def __init__(self, rpc_urls: dict[str, str], timeout_seconds: float = 15.0) -> None:
        self.rpc_urls = {key: value for key, value in rpc_urls.items() if value}
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _rpc(self, chain_code: str, method: str, params: list[object]) -> str:
        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing RPC URL for chain '{chain_code}'")

        response = self._client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))

        result = payload.get("result")
        if not isinstance(result, str):
            raise RuntimeError(f"unexpected RPC response for {method}: {payload}")
        return result

    def _eth_call_words(self, chain_code: str, to: str, data: str) -> list[int]:
        raw_hex = self._rpc(chain_code, "eth_call", [{"to": to, "data": data}, "latest"])
        return _decode_words(raw_hex)

    def get_block_number(self, chain_code: str) -> int:
        raw_hex = self._rpc(chain_code, "eth_blockNumber", [])
        return int(raw_hex, 16)

    def get_token_x(self, chain_code: str, pool_address: str) -> str:
        words = self._eth_call_words(chain_code, pool_address, LB_GET_TOKEN_X_SELECTOR)
        if not words:
            raise RuntimeError("Trader Joe LB getTokenX() returned empty response")
        return _decode_address_word(words[0])

    def get_token_y(self, chain_code: str, pool_address: str) -> str:
        words = self._eth_call_words(chain_code, pool_address, LB_GET_TOKEN_Y_SELECTOR)
        if not words:
            raise RuntimeError("Trader Joe LB getTokenY() returned empty response")
        return _decode_address_word(words[0])

    def get_bin(self, chain_code: str, pool_address: str, bin_id: int) -> tuple[int, int]:
        data = f"{LB_GET_BIN_SELECTOR}{_encode_uint(bin_id)}"
        words = self._eth_call_words(chain_code, pool_address, data)
        if len(words) < 2:
            raise RuntimeError("Trader Joe LB getBin() returned insufficient words")
        return words[0], words[1]

    def get_bin_total_supply(self, chain_code: str, pool_address: str, bin_id: int) -> int:
        data = f"{LB_TOTAL_SUPPLY_SELECTOR}{_encode_uint(bin_id)}"
        words = self._eth_call_words(chain_code, pool_address, data)
        if not words:
            raise RuntimeError("Trader Joe LB totalSupply(id) returned empty response")
        return words[0]

    def get_bin_balance(
        self, chain_code: str, pool_address: str, wallet_address: str, bin_id: int
    ) -> int:
        data = f"{LB_BALANCE_OF_SELECTOR}{_encode_address(wallet_address)}{_encode_uint(bin_id)}"
        words = self._eth_call_words(chain_code, pool_address, data)
        if not words:
            raise RuntimeError("Trader Joe LB balanceOf(address,id) returned empty response")
        return words[0]


@dataclass(frozen=True)
class _BinState:
    bin_id: int
    reserve_x_raw: int
    reserve_y_raw: int
    total_supply_raw: int


class TraderJoeLpAdapter:
    """Collect canonical Trader Joe LB positions for configured buy-wall pools."""

    protocol_code = "traderjoe_lp"

    def __init__(self, markets_config: MarketsConfig, rpc_client: TraderJoeLpRpcClient) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, pool_address: str) -> str:
        return f"traderjoe_lp:{chain_code}:{wallet_address}:{pool_address}"

    def _issue(
        self,
        *,
        as_of_ts_utc: datetime,
        error_type: str,
        error_message: str,
        chain_code: str,
        wallet_address: str | None = None,
        market_ref: str | None = None,
        payload_json: dict[str, object] | None = None,
    ) -> DataQualityIssue:
        return DataQualityIssue(
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_snapshot",
            error_type=error_type,
            error_message=error_message,
            protocol_code=self.protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
            market_ref=market_ref,
            payload_json=payload_json,
        )

    def _build_bin_states(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        pool: TraderJoePool,
        market_ref: str,
        issues: list[DataQualityIssue],
    ) -> list[_BinState]:
        bin_states: list[_BinState] = []
        for bin_id in pool.bin_ids:
            try:
                reserve_x_raw, reserve_y_raw = self.rpc_client.get_bin(
                    chain_code,
                    market_ref,
                    bin_id,
                )
                total_supply_raw = self.rpc_client.get_bin_total_supply(
                    chain_code,
                    market_ref,
                    bin_id,
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="traderjoe_bin_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"bin_id": bin_id},
                    )
                )
                continue

            if total_supply_raw <= 0:
                continue

            bin_states.append(
                _BinState(
                    bin_id=bin_id,
                    reserve_x_raw=reserve_x_raw,
                    reserve_y_raw=reserve_y_raw,
                    total_supply_raw=total_supply_raw,
                )
            )
        return bin_states

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.traderjoe_lp.items():
            block_number_or_slot: str | None = None
            try:
                block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="traderjoe_block_number_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

            for pool in chain_config.pools:
                market_ref = canonical_address(pool.pool_address)

                if pool.pool_type != "joe_v2_lb":
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="traderjoe_pool_type_unsupported",
                            error_message=(
                                f"unsupported traderjoe pool_type='{pool.pool_type}' for current "
                                "adapter"
                            ),
                            chain_code=chain_code,
                            market_ref=market_ref,
                        )
                    )
                    continue

                try:
                    observed_token_x = canonical_address(
                        self.rpc_client.get_token_x(chain_code, market_ref)
                    )
                    observed_token_y = canonical_address(
                        self.rpc_client.get_token_y(chain_code, market_ref)
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="traderjoe_pool_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"pool_type": pool.pool_type},
                        )
                    )
                    continue

                configured_token_x = canonical_address(pool.token_x_address)
                configured_token_y = canonical_address(pool.token_y_address)
                if observed_token_x != configured_token_x or observed_token_y != configured_token_y:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="traderjoe_token_mismatch",
                            error_message=(
                                "Trader Joe LB token metadata differs between config and on-chain "
                                "reads"
                            ),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "configured_token_x": configured_token_x,
                                "configured_token_y": configured_token_y,
                                "observed_token_x": observed_token_x,
                                "observed_token_y": observed_token_y,
                            },
                        )
                    )

                bin_states = self._build_bin_states(
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code=chain_code,
                    pool=pool,
                    market_ref=market_ref,
                    issues=issues,
                )
                if not bin_states:
                    continue

                token_x_price = prices_by_token.get((chain_code, configured_token_x))
                token_y_price = prices_by_token.get((chain_code, configured_token_y))
                if token_x_price is None:
                    token_x_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="price_missing",
                            error_message="no price available for Trader Joe token X",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "token_address": configured_token_x,
                                "symbol": pool.token_x_symbol,
                            },
                        )
                    )
                if token_y_price is None:
                    token_y_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="price_missing",
                            error_message="no price available for Trader Joe token Y",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "token_address": configured_token_y,
                                "symbol": pool.token_y_symbol,
                            },
                        )
                    )

                for wallet in chain_config.wallets:
                    wallet_address = canonical_address(wallet)
                    user_x_raw = Decimal("0")
                    user_y_raw = Decimal("0")

                    for bin_state in bin_states:
                        try:
                            user_shares_raw = self.rpc_client.get_bin_balance(
                                chain_code,
                                market_ref,
                                wallet_address,
                                bin_state.bin_id,
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    error_type="traderjoe_position_read_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=market_ref,
                                    payload_json={"bin_id": bin_state.bin_id},
                                )
                            )
                            continue

                        if user_shares_raw <= 0:
                            continue

                        share_ratio = Decimal(user_shares_raw) / Decimal(bin_state.total_supply_raw)
                        user_x_raw += Decimal(bin_state.reserve_x_raw) * share_ratio
                        user_y_raw += Decimal(bin_state.reserve_y_raw) * share_ratio

                    if user_x_raw <= 0 and user_y_raw <= 0:
                        continue

                    supplied_amount_x = normalize_raw_amount(user_x_raw, pool.token_x_decimals)
                    supplied_amount_y = normalize_raw_amount(user_y_raw, pool.token_y_decimals)
                    supplied_usd = (supplied_amount_x * token_x_price) + (
                        supplied_amount_y * token_y_price
                    )

                    positions.append(
                        PositionSnapshotInput(
                            as_of_ts_utc=as_of_ts_utc,
                            protocol_code=self.protocol_code,
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            position_key=self._position_key(chain_code, wallet_address, market_ref),
                            # Normalize to token Y units for the canonical amount field.
                            supplied_amount=supplied_amount_y,
                            supplied_usd=supplied_usd,
                            borrowed_amount=Decimal("0"),
                            borrowed_usd=Decimal("0"),
                            supply_apy=Decimal("0"),
                            borrow_apy=Decimal("0"),
                            reward_apy=Decimal("0"),
                            equity_usd=supplied_usd,
                            source="rpc",
                            block_number_or_slot=block_number_or_slot,
                        )
                    )

        return positions, issues
