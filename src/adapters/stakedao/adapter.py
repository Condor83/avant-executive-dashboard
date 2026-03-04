"""Stake DAO vault adapter with Curve LP underlying decomposition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import MarketsConfig, StakedaoUnderlyingToken, StakedaoVault, canonical_address
from core.types import DataQualityIssue, PositionSnapshotInput

BALANCE_OF_SELECTOR = "0x70a08231"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"
CURVE_BALANCES_SELECTOR = "0x4903b0d1"
CURVE_COINS_SELECTOR = "0xc6610657"


def normalize_raw_amount(raw_amount: Decimal | int, decimals: int) -> Decimal:
    """Convert raw token balance into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _strip_0x_hex(value: str) -> str:
    cleaned = value.strip().lower()
    return cleaned[2:] if cleaned.startswith("0x") else cleaned


def _encode_address(address: str) -> str:
    return _strip_0x_hex(address).rjust(64, "0")


def _encode_uint(value: int) -> str:
    return hex(value)[2:].rjust(64, "0")


def _decode_words(raw_hex: str) -> list[int]:
    payload = _strip_0x_hex(raw_hex)
    if not payload:
        return []
    if len(payload) % 64 != 0:
        raise RuntimeError(f"invalid ABI payload length={len(payload)}")
    return [int(payload[idx : idx + 64], 16) for idx in range(0, len(payload), 64)]


def _decode_address_word(word: int) -> str:
    return f"0x{word.to_bytes(32, 'big')[-20:].hex()}"


class StakedaoRpcClient(Protocol):
    """RPC contract required by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest block number."""

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        """Return ERC20 balanceOf(wallet)."""

    def convert_to_assets(self, chain_code: str, vault_address: str, shares_raw: int) -> int:
        """Return ERC4626 convertToAssets(shares)."""

    def get_total_supply(self, chain_code: str, token_address: str) -> int:
        """Return ERC20 totalSupply()."""

    def get_curve_balance(self, chain_code: str, pool_address: str, index: int) -> int:
        """Return Curve-like balances(index)."""

    def get_curve_coin(self, chain_code: str, pool_address: str, index: int) -> str:
        """Return Curve-like coins(index)."""


class EvmRpcStakedaoClient:
    """Minimal JSON-RPC client for Stake DAO + Curve calls."""

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

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        words = self._eth_call_words(
            chain_code,
            token_address,
            f"{BALANCE_OF_SELECTOR}{_encode_address(wallet_address)}",
        )
        if not words:
            raise RuntimeError("balanceOf returned empty response")
        return words[0]

    def convert_to_assets(self, chain_code: str, vault_address: str, shares_raw: int) -> int:
        words = self._eth_call_words(
            chain_code,
            vault_address,
            f"{CONVERT_TO_ASSETS_SELECTOR}{_encode_uint(shares_raw)}",
        )
        if not words:
            raise RuntimeError("convertToAssets returned empty response")
        return words[0]

    def get_total_supply(self, chain_code: str, token_address: str) -> int:
        words = self._eth_call_words(chain_code, token_address, TOTAL_SUPPLY_SELECTOR)
        if not words:
            raise RuntimeError("totalSupply returned empty response")
        return words[0]

    def get_curve_balance(self, chain_code: str, pool_address: str, index: int) -> int:
        words = self._eth_call_words(
            chain_code,
            pool_address,
            f"{CURVE_BALANCES_SELECTOR}{_encode_uint(index)}",
        )
        if not words:
            raise RuntimeError("balances(uint256) returned empty response")
        return words[0]

    def get_curve_coin(self, chain_code: str, pool_address: str, index: int) -> str:
        words = self._eth_call_words(
            chain_code,
            pool_address,
            f"{CURVE_COINS_SELECTOR}{_encode_uint(index)}",
        )
        if not words:
            raise RuntimeError("coins(uint256) returned empty response")
        return _decode_address_word(words[0])


@dataclass(frozen=True)
class _UnderlyingPoolState:
    token: StakedaoUnderlyingToken
    pool_balance_raw: int


class StakedaoAdapter:
    """Collect Stake DAO vault balances normalized to underlying supply tokens."""

    protocol_code = "stakedao"

    def __init__(self, markets_config: MarketsConfig, rpc_client: StakedaoRpcClient) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client

    @staticmethod
    def _market_ref(vault_address: str, token_address: str) -> str:
        return f"{canonical_address(vault_address)}:{canonical_address(token_address)}"

    @staticmethod
    def _position_key(
        chain_code: str, wallet_address: str, vault_address: str, token_address: str
    ) -> str:
        return (
            f"stakedao:{chain_code}:{wallet_address}:{canonical_address(vault_address)}:"
            f"{canonical_address(token_address)}"
        )

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

    def _load_pool_state(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        vault: StakedaoVault,
        issues: list[DataQualityIssue],
    ) -> tuple[int | None, list[_UnderlyingPoolState]]:
        pool_address = canonical_address(vault.asset_address)

        try:
            total_supply_raw = self.rpc_client.get_total_supply(chain_code, pool_address)
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    error_type="stakedao_pool_read_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                    market_ref=pool_address,
                )
            )
            return None, []

        state: list[_UnderlyingPoolState] = []
        for underlying in vault.underlyings:
            market_ref = self._market_ref(vault.vault_address, underlying.address)
            try:
                observed_coin = canonical_address(
                    self.rpc_client.get_curve_coin(
                        chain_code,
                        pool_address,
                        underlying.pool_index,
                    )
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="stakedao_pool_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"pool_index": underlying.pool_index},
                    )
                )
                continue

            expected_coin = canonical_address(underlying.address)
            if observed_coin != expected_coin:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="stakedao_pool_token_mismatch",
                        error_message="configured underlying token does not match pool coin()",
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={
                            "pool_index": underlying.pool_index,
                            "expected": expected_coin,
                            "observed": observed_coin,
                        },
                    )
                )
                continue

            try:
                pool_balance_raw = self.rpc_client.get_curve_balance(
                    chain_code,
                    pool_address,
                    underlying.pool_index,
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="stakedao_pool_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"pool_index": underlying.pool_index},
                    )
                )
                continue

            state.append(
                _UnderlyingPoolState(
                    token=underlying,
                    pool_balance_raw=pool_balance_raw,
                )
            )

        return total_supply_raw, state

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.stakedao.items():
            block_number_or_slot: str | None = None
            try:
                block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="stakedao_block_number_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

            for vault in chain_config.vaults:
                vault_address = canonical_address(vault.vault_address)
                total_supply_raw, pool_state = self._load_pool_state(
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code=chain_code,
                    vault=vault,
                    issues=issues,
                )
                if not pool_state or total_supply_raw is None or total_supply_raw <= 0:
                    continue

                for wallet in chain_config.wallets:
                    wallet_address = canonical_address(wallet)
                    try:
                        shares_raw = self.rpc_client.get_erc20_balance(
                            chain_code,
                            vault_address,
                            wallet_address,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                error_type="stakedao_position_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=vault_address,
                            )
                        )
                        continue

                    if shares_raw <= 0:
                        continue

                    try:
                        assets_raw = self.rpc_client.convert_to_assets(
                            chain_code,
                            vault_address,
                            shares_raw,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                error_type="stakedao_position_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=vault_address,
                            )
                        )
                        continue

                    if assets_raw <= 0:
                        continue

                    ownership = Decimal(assets_raw) / Decimal(total_supply_raw)
                    for underlying_state in pool_state:
                        token = underlying_state.token
                        token_address = canonical_address(token.address)
                        supplied_raw = Decimal(underlying_state.pool_balance_raw) * ownership
                        supplied_amount = normalize_raw_amount(supplied_raw, token.decimals)
                        if supplied_amount <= 0:
                            continue

                        price_usd = prices_by_token.get((chain_code, token_address))
                        if price_usd is None:
                            price_usd = Decimal("0")
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    error_type="price_missing",
                                    error_message=(
                                        "no price available for stakedao underlying token"
                                    ),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=self._market_ref(vault_address, token_address),
                                    payload_json={"symbol": token.symbol},
                                )
                            )

                        supplied_usd = supplied_amount * price_usd
                        positions.append(
                            PositionSnapshotInput(
                                as_of_ts_utc=as_of_ts_utc,
                                protocol_code=self.protocol_code,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=self._market_ref(vault_address, token_address),
                                position_key=self._position_key(
                                    chain_code,
                                    wallet_address,
                                    vault_address,
                                    token_address,
                                ),
                                supplied_amount=supplied_amount,
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
