"""Etherex concentrated-liquidity adapter for canonical position ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Protocol

import httpx

from core.config import EtherexPool, MarketsConfig, canonical_address
from core.types import DataQualityIssue, PositionSnapshotInput

BALANCE_OF_SELECTOR = "0x70a08231"
TOKEN_OF_OWNER_BY_INDEX_SELECTOR = "0x2f745c59"
POSITIONS_SELECTOR = "0x99fbab88"
SLOT0_SELECTOR = "0x3850c7bd"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"

Q96 = Decimal(2) ** 96
SYMBOL_PRICE_EQUIVALENTS: dict[str, tuple[str, ...]] = {
    "AVUSD": ("SAVUSD",),
    "SAVUSD": ("AVUSD",),
}


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
    return [int(payload[idx : idx + 64], 16) for idx in range(0, len(payload), 64)]


def _decode_address_word(word: int) -> str:
    return f"0x{word.to_bytes(32, 'big')[-20:].hex()}"


def _decode_signed_int(word: int, bits: int) -> int:
    mask = (1 << bits) - 1
    value = word & mask
    if value >= (1 << (bits - 1)):
        value -= 1 << bits
    return value


def _sqrt_ratio_at_tick(tick: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 80
        return (Decimal("1.0001") ** (Decimal(tick) / Decimal(2))) * Q96


def _liquidity_to_amounts(
    *,
    liquidity: int,
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
) -> tuple[Decimal, Decimal]:
    """Return token0/token1 raw amounts implied by V3 liquidity at current price."""

    if liquidity <= 0:
        return Decimal("0"), Decimal("0")

    with localcontext() as ctx:
        ctx.prec = 90

        sqrt_price = Decimal(sqrt_price_x96)
        sqrt_lower = _sqrt_ratio_at_tick(tick_lower)
        sqrt_upper = _sqrt_ratio_at_tick(tick_upper)
        liquidity_decimal = Decimal(liquidity)

        if sqrt_price <= sqrt_lower:
            amount0 = (
                liquidity_decimal * (sqrt_upper - sqrt_lower) * Q96 / (sqrt_upper * sqrt_lower)
            )
            amount1 = Decimal("0")
            return amount0, amount1

        if sqrt_price < sqrt_upper:
            amount0 = (
                liquidity_decimal * (sqrt_upper - sqrt_price) * Q96 / (sqrt_upper * sqrt_price)
            )
            amount1 = liquidity_decimal * (sqrt_price - sqrt_lower) / Q96
            return amount0, amount1

        amount0 = Decimal("0")
        amount1 = liquidity_decimal * (sqrt_upper - sqrt_lower) / Q96
        return amount0, amount1


@dataclass(frozen=True)
class EtherexPosition:
    token0_address: str
    token1_address: str
    fee: int
    tick_lower: int
    tick_upper: int
    liquidity: int
    tokens_owed_0: int
    tokens_owed_1: int


class EtherexRpcClient(Protocol):
    """Protocol for Etherex CL reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_pool_token0(self, chain_code: str, pool_address: str) -> str:
        """Return pool token0 address."""

    def get_pool_token1(self, chain_code: str, pool_address: str) -> str:
        """Return pool token1 address."""

    def get_pool_fee(self, chain_code: str, pool_address: str) -> int:
        """Return pool fee tier."""

    def get_pool_slot0(self, chain_code: str, pool_address: str) -> tuple[int, int]:
        """Return `(sqrt_price_x96, tick)` for the pool."""

    def get_balance_of(self, chain_code: str, manager_address: str, owner: str) -> int:
        """Return number of position NFTs held by owner."""

    def get_token_of_owner_by_index(
        self,
        chain_code: str,
        manager_address: str,
        owner: str,
        index: int,
    ) -> int:
        """Return NFT token id at owner index."""

    def get_position(self, chain_code: str, manager_address: str, token_id: int) -> EtherexPosition:
        """Return decoded position metadata for token id."""


class EvmRpcEtherexClient:
    """JSON-RPC client for Etherex concentrated-liquidity pool reads."""

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

    def get_pool_token0(self, chain_code: str, pool_address: str) -> str:
        words = self._eth_call_words(chain_code, pool_address, TOKEN0_SELECTOR)
        if not words:
            raise RuntimeError("Etherex pool token0() returned empty response")
        return _decode_address_word(words[0])

    def get_pool_token1(self, chain_code: str, pool_address: str) -> str:
        words = self._eth_call_words(chain_code, pool_address, TOKEN1_SELECTOR)
        if not words:
            raise RuntimeError("Etherex pool token1() returned empty response")
        return _decode_address_word(words[0])

    def get_pool_fee(self, chain_code: str, pool_address: str) -> int:
        words = self._eth_call_words(chain_code, pool_address, FEE_SELECTOR)
        if not words:
            raise RuntimeError("Etherex pool fee() returned empty response")
        return words[0]

    def get_pool_slot0(self, chain_code: str, pool_address: str) -> tuple[int, int]:
        words = self._eth_call_words(chain_code, pool_address, SLOT0_SELECTOR)
        if len(words) < 2:
            raise RuntimeError("Etherex pool slot0() returned insufficient words")
        return words[0], _decode_signed_int(words[1], 24)

    def get_balance_of(self, chain_code: str, manager_address: str, owner: str) -> int:
        data = f"{BALANCE_OF_SELECTOR}{_encode_address(owner)}"
        words = self._eth_call_words(chain_code, manager_address, data)
        if not words:
            raise RuntimeError("position manager balanceOf() returned empty response")
        return words[0]

    def get_token_of_owner_by_index(
        self,
        chain_code: str,
        manager_address: str,
        owner: str,
        index: int,
    ) -> int:
        data = f"{TOKEN_OF_OWNER_BY_INDEX_SELECTOR}{_encode_address(owner)}{_encode_uint(index)}"
        words = self._eth_call_words(chain_code, manager_address, data)
        if not words:
            raise RuntimeError("position manager tokenOfOwnerByIndex() returned empty response")
        return words[0]

    def get_position(self, chain_code: str, manager_address: str, token_id: int) -> EtherexPosition:
        data = f"{POSITIONS_SELECTOR}{_encode_uint(token_id)}"
        words = self._eth_call_words(chain_code, manager_address, data)
        if len(words) < 8:
            raise RuntimeError("position manager positions() returned insufficient words")

        return EtherexPosition(
            token0_address=_decode_address_word(words[0]),
            token1_address=_decode_address_word(words[1]),
            fee=words[2],
            tick_lower=_decode_signed_int(words[3], 24),
            tick_upper=_decode_signed_int(words[4], 24),
            liquidity=words[5],
            tokens_owed_0=words[6],
            tokens_owed_1=words[7],
        )


class EtherexAdapter:
    """Collect canonical Etherex concentrated-liquidity positions."""

    protocol_code = "etherex"

    def __init__(self, markets_config: MarketsConfig, rpc_client: EtherexRpcClient) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client

    @staticmethod
    def _position_key(
        chain_code: str,
        wallet_address: str,
        pool_address: str,
        token_id: int,
    ) -> str:
        return f"etherex:{chain_code}:{wallet_address}:{pool_address}:{token_id}"

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

    @staticmethod
    def _pool_fee_matches_config(*, observed_pool_fee: int, configured_position_fee: int) -> bool:
        if observed_pool_fee == configured_position_fee:
            return True
        # Etherex pool fee() has been observed in 1e4-like units while NFT positions()
        # expose a smaller tier integer (for example 250 vs 5).
        if observed_pool_fee == configured_position_fee * 50:
            return True
        return False

    @staticmethod
    def _resolve_price(
        *,
        prices_by_token: dict[tuple[str, str], Decimal],
        chain_code: str,
        token_address: str,
        symbol: str,
    ) -> Decimal | None:
        direct = prices_by_token.get((chain_code, token_address))
        if direct is not None:
            return direct

        normalized_symbol = symbol.strip().upper()
        symbol_candidates = [
            normalized_symbol,
            *SYMBOL_PRICE_EQUIVALENTS.get(normalized_symbol, ()),
        ]
        symbol_keys = {f"symbol:{candidate}" for candidate in symbol_candidates}

        same_chain_prices: list[Decimal] = []
        cross_chain_prices: list[Decimal] = []
        for (candidate_chain, candidate_key), price in prices_by_token.items():
            if candidate_key not in symbol_keys:
                continue
            if candidate_chain == chain_code:
                same_chain_prices.append(price)
            else:
                cross_chain_prices.append(price)

        candidates = same_chain_prices or cross_chain_prices
        if not candidates:
            return None
        if normalized_symbol == "AVUSD":
            return min(candidates)
        if normalized_symbol == "SAVUSD":
            return max(candidates)
        return candidates[0]

    def _validate_pool_surface(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        pool: EtherexPool,
        market_ref: str,
        issues: list[DataQualityIssue],
    ) -> bool:
        configured_token0 = canonical_address(pool.token0_address)
        configured_token1 = canonical_address(pool.token1_address)

        try:
            observed_token0 = canonical_address(
                self.rpc_client.get_pool_token0(chain_code, market_ref)
            )
            observed_token1 = canonical_address(
                self.rpc_client.get_pool_token1(chain_code, market_ref)
            )
            observed_fee = int(self.rpc_client.get_pool_fee(chain_code, market_ref))
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    error_type="etherex_pool_read_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                    market_ref=market_ref,
                )
            )
            return False

        if (
            observed_token0 != configured_token0
            or observed_token1 != configured_token1
            or not self._pool_fee_matches_config(
                observed_pool_fee=observed_fee,
                configured_position_fee=int(pool.fee),
            )
        ):
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    error_type="etherex_pool_mismatch",
                    error_message="Etherex pool metadata differs between config and on-chain reads",
                    chain_code=chain_code,
                    market_ref=market_ref,
                    payload_json={
                        "configured_token0": configured_token0,
                        "configured_token1": configured_token1,
                        "configured_fee": int(pool.fee),
                        "observed_token0": observed_token0,
                        "observed_token1": observed_token1,
                        "observed_fee": observed_fee,
                    },
                )
            )
            return False

        return True

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.etherex.items():
            block_number_or_slot: str | None = None
            try:
                block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        error_type="etherex_block_number_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

            for pool in chain_config.pools:
                market_ref = canonical_address(pool.pool_address)
                manager_address = canonical_address(pool.position_manager_address)
                if not self._validate_pool_surface(
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code=chain_code,
                    pool=pool,
                    market_ref=market_ref,
                    issues=issues,
                ):
                    continue

                try:
                    sqrt_price_x96, _tick = self.rpc_client.get_pool_slot0(chain_code, market_ref)
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="etherex_slot0_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                        )
                    )
                    continue

                token0_address = canonical_address(pool.token0_address)
                token1_address = canonical_address(pool.token1_address)

                token0_price = self._resolve_price(
                    prices_by_token=prices_by_token,
                    chain_code=chain_code,
                    token_address=token0_address,
                    symbol=pool.token0_symbol,
                )
                if token0_price is None:
                    token0_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="price_missing",
                            error_message="no price available for Etherex token0",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "token_address": token0_address,
                                "symbol": pool.token0_symbol,
                            },
                        )
                    )

                token1_price = self._resolve_price(
                    prices_by_token=prices_by_token,
                    chain_code=chain_code,
                    token_address=token1_address,
                    symbol=pool.token1_symbol,
                )
                if token1_price is None:
                    token1_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            error_type="price_missing",
                            error_message="no price available for Etherex token1",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "token_address": token1_address,
                                "symbol": pool.token1_symbol,
                            },
                        )
                    )

                for wallet in chain_config.wallets:
                    wallet_address = canonical_address(wallet)
                    try:
                        nft_count = self.rpc_client.get_balance_of(
                            chain_code,
                            manager_address,
                            wallet_address,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                error_type="etherex_balance_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                            )
                        )
                        continue

                    for index in range(nft_count):
                        try:
                            token_id = self.rpc_client.get_token_of_owner_by_index(
                                chain_code,
                                manager_address,
                                wallet_address,
                                index,
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    error_type="etherex_token_enumeration_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=market_ref,
                                    payload_json={"owner_index": index},
                                )
                            )
                            continue

                        try:
                            position = self.rpc_client.get_position(
                                chain_code,
                                manager_address,
                                token_id,
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    error_type="etherex_position_read_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=market_ref,
                                    payload_json={"token_id": token_id},
                                )
                            )
                            continue

                        if (
                            canonical_address(position.token0_address) != token0_address
                            or canonical_address(position.token1_address) != token1_address
                            or int(position.fee) != int(pool.fee)
                        ):
                            continue

                        amount0_raw, amount1_raw = _liquidity_to_amounts(
                            liquidity=position.liquidity,
                            sqrt_price_x96=sqrt_price_x96,
                            tick_lower=position.tick_lower,
                            tick_upper=position.tick_upper,
                        )
                        amount0_raw += Decimal(position.tokens_owed_0)
                        amount1_raw += Decimal(position.tokens_owed_1)

                        if amount0_raw <= 0 and amount1_raw <= 0:
                            continue

                        supplied_amount_token0 = normalize_raw_amount(
                            amount0_raw,
                            pool.token0_decimals,
                        )
                        supplied_amount_token1 = normalize_raw_amount(
                            amount1_raw,
                            pool.token1_decimals,
                        )
                        supplied_usd = (supplied_amount_token0 * token0_price) + (
                            supplied_amount_token1 * token1_price
                        )

                        positions.append(
                            PositionSnapshotInput(
                                as_of_ts_utc=as_of_ts_utc,
                                protocol_code=self.protocol_code,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                position_key=self._position_key(
                                    chain_code,
                                    wallet_address,
                                    market_ref,
                                    token_id,
                                ),
                                supplied_amount=supplied_amount_token0,
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
