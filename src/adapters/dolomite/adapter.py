"""Dolomite adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import DolomiteChainConfig, DolomiteMarket, MarketsConfig, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput
from core.yields import AVANT_APY_ENDPOINTS, AvantYieldOracle, DefiLlamaYieldOracle

DOLO_GET_ACCOUNT_WEI_SELECTOR = "0xc190c2ec"
DOLO_GET_MARKET_TOKEN_ADDRESS_SELECTOR = "0x062bd3e9"
DOLO_GET_MARKET_CURRENT_INDEX_SELECTOR = "0x56ea84b2"
DOLO_GET_MARKET_TOTAL_PAR_SELECTOR = "0xcb04a34c"
DOLO_GET_MARKET_INTEREST_RATE_SELECTOR = "0xfd47eda6"
DOLO_GET_MARKET_PRICE_SELECTOR = "0x8928378e"
DOLO_GET_NUM_MARKETS_SELECTOR = "0x295c39a5"
ERC20_DECIMALS_SELECTOR = "0x313ce567"

WAD = Decimal("1e18")
SECONDS_PER_YEAR_FLOAT = 31536000.0


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
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


def _safe_apy_from_per_second(rate_per_second: Decimal) -> Decimal:
    """Convert a per-second decimal rate into annual APY (0.0-1.0 units)."""

    if rate_per_second <= 0:
        return Decimal("0")
    try:
        apy_float = math.expm1(float(rate_per_second) * SECONDS_PER_YEAR_FLOAT)
    except (OverflowError, ValueError):
        return Decimal("0")
    if apy_float <= 0:
        return Decimal("0")
    return Decimal(str(apy_float))


@dataclass(frozen=True)
class DolomiteSignedWei:
    is_positive: bool
    value: int


@dataclass(frozen=True)
class DolomiteMarketIndex:
    borrow: int
    supply: int
    last_update: int


@dataclass(frozen=True)
class DolomiteMarketPar:
    borrow: int
    supply: int


@dataclass(frozen=True)
class DolomiteMarketState:
    token_address: str
    price_raw: int
    interest_rate_raw: int
    current_index: DolomiteMarketIndex
    total_par: DolomiteMarketPar


@dataclass(frozen=True)
class _DolomiteMarketRuntime:
    config: DolomiteMarket
    market_ref: str
    state: DolomiteMarketState
    price_usd: Decimal
    borrow_rate_per_second: Decimal
    supply_rate_per_second: Decimal
    borrow_apy: Decimal
    supply_apy: Decimal


class DolomiteRpcClient(Protocol):
    """Protocol for Dolomite reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        """Return token address for a Dolomite market id."""

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        """Return total number of market ids available on Dolomite margin."""

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        """Return market price raw value for a Dolomite market id."""

    def get_market_interest_rate(self, chain_code: str, margin_address: str, market_id: int) -> int:
        """Return per-second borrow rate (WAD)."""

    def get_market_current_index(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketIndex:
        """Return market supply/borrow index values."""

    def get_market_total_par(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketPar:
        """Return market total borrow/supply principal balances."""

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        """Return signed wei balance for wallet/account/market."""

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        """Return ERC20 decimals for a token."""


class EvmRpcDolomiteClient:
    """JSON-RPC client for Dolomite calls against EVM chains."""

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

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        data = f"{DOLO_GET_MARKET_TOKEN_ADDRESS_SELECTOR}{_encode_uint(market_id)}"
        words = self._eth_call_words(chain_code, margin_address, data)
        if not words:
            raise RuntimeError("Dolomite getMarketTokenAddress returned empty response")
        return _decode_address_word(words[0])

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        words = self._eth_call_words(chain_code, margin_address, DOLO_GET_NUM_MARKETS_SELECTOR)
        if not words:
            raise RuntimeError("Dolomite getNumMarkets returned empty response")
        return int(words[0])

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        data = f"{DOLO_GET_MARKET_PRICE_SELECTOR}{_encode_uint(market_id)}"
        words = self._eth_call_words(chain_code, margin_address, data)
        if not words:
            raise RuntimeError("Dolomite getMarketPrice returned empty response")
        return words[0]

    def get_market_interest_rate(self, chain_code: str, margin_address: str, market_id: int) -> int:
        data = f"{DOLO_GET_MARKET_INTEREST_RATE_SELECTOR}{_encode_uint(market_id)}"
        words = self._eth_call_words(chain_code, margin_address, data)
        if not words:
            raise RuntimeError("Dolomite getMarketInterestRate returned empty response")
        return words[0]

    def get_market_current_index(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketIndex:
        data = f"{DOLO_GET_MARKET_CURRENT_INDEX_SELECTOR}{_encode_uint(market_id)}"
        words = self._eth_call_words(chain_code, margin_address, data)
        if len(words) < 3:
            raise RuntimeError("Dolomite getMarketCurrentIndex returned insufficient words")
        return DolomiteMarketIndex(borrow=words[0], supply=words[1], last_update=words[2])

    def get_market_total_par(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketPar:
        data = f"{DOLO_GET_MARKET_TOTAL_PAR_SELECTOR}{_encode_uint(market_id)}"
        words = self._eth_call_words(chain_code, margin_address, data)
        if len(words) < 2:
            raise RuntimeError("Dolomite getMarketTotalPar returned insufficient words")
        return DolomiteMarketPar(borrow=words[0], supply=words[1])

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        data = DOLO_GET_ACCOUNT_WEI_SELECTOR
        data += _encode_address(wallet_address)
        data += _encode_uint(account_number)
        data += _encode_uint(market_id)
        words = self._eth_call_words(chain_code, margin_address, data)
        if len(words) < 2:
            raise RuntimeError("Dolomite getAccountWei returned insufficient words")
        return DolomiteSignedWei(is_positive=bool(words[0]), value=words[1])

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        words = self._eth_call_words(chain_code, token_address, ERC20_DECIMALS_SELECTOR)
        if not words:
            raise RuntimeError("ERC20 decimals call returned empty response")
        return int(words[0])


class DolomiteAdapter:
    """Collect canonical Dolomite positions and market snapshots."""

    protocol_code = "dolomite"

    def __init__(
        self,
        markets_config: MarketsConfig,
        rpc_client: DolomiteRpcClient,
        avant_yield_oracle: AvantYieldOracle | None = None,
        yield_oracle: DefiLlamaYieldOracle | None = None,
    ) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client
        self.avant_yield_oracle = avant_yield_oracle
        self.yield_oracle = yield_oracle

    @staticmethod
    def _position_key(
        chain_code: str,
        wallet_address: str,
        account_number: int,
        market_ref: str,
    ) -> str:
        return f"dolomite:{chain_code}:{wallet_address}:{account_number}:{market_ref}"

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

    @staticmethod
    def _price_lookup_key(chain_code: str, token_address: str) -> tuple[str, str]:
        return (chain_code, canonical_address(token_address))

    @staticmethod
    def _normalize_price(price_raw: int, decimals: int) -> Decimal:
        exponent = 36 - decimals
        if exponent <= 0:
            return Decimal(price_raw)
        scale = Decimal(10) ** Decimal(exponent)
        return Decimal(price_raw) / scale

    @staticmethod
    def _par_to_wei(par: int, index: int) -> int:
        if par <= 0 or index <= 0:
            return 0
        return (par * index) // int(WAD)

    def _resolve_price_usd(
        self,
        *,
        chain_code: str,
        runtime: _DolomiteMarketRuntime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> Decimal:
        configured_token_address = runtime.config.token_address
        if configured_token_address is not None:
            price = prices_by_token.get(
                self._price_lookup_key(chain_code, configured_token_address)
            )
            if price is not None:
                return price

        live_price = prices_by_token.get(
            self._price_lookup_key(chain_code, runtime.state.token_address)
        )
        if live_price is not None:
            return live_price

        symbol_price = prices_by_token.get((chain_code, f"symbol:{runtime.config.symbol.upper()}"))
        if symbol_price is not None:
            return symbol_price

        return runtime.price_usd

    @staticmethod
    def _supports_avant_native_yield(symbol: str) -> bool:
        return symbol.strip().upper() in AVANT_APY_ENDPOINTS

    def _issue(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        error_type: str,
        error_message: str,
        chain_code: str,
        wallet_address: str | None = None,
        market_ref: str | None = None,
        payload_json: dict[str, object] | None = None,
    ) -> DataQualityIssue:
        return DataQualityIssue(
            as_of_ts_utc=as_of_ts_utc,
            stage=stage,
            error_type=error_type,
            error_message=error_message,
            protocol_code=self.protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
            market_ref=market_ref,
            payload_json=payload_json,
        )

    def _collect_chain_runtime(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        chain_code: str,
        chain_config: DolomiteChainConfig,
    ) -> tuple[dict[int, _DolomiteMarketRuntime], list[DataQualityIssue], str | None]:
        runtimes: dict[int, _DolomiteMarketRuntime] = {}
        issues: list[DataQualityIssue] = []

        block_number_or_slot: str | None = None
        try:
            block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="dolomite_block_number_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                )
            )

        margin_address = canonical_address(chain_config.margin)
        for market in chain_config.markets:
            market_ref = str(market.id)
            try:
                token_address = canonical_address(
                    self.rpc_client.get_market_token_address(chain_code, margin_address, market.id)
                )
                price_raw = self.rpc_client.get_market_price(chain_code, margin_address, market.id)
                interest_rate_raw = self.rpc_client.get_market_interest_rate(
                    chain_code,
                    margin_address,
                    market.id,
                )
                current_index = self.rpc_client.get_market_current_index(
                    chain_code,
                    margin_address,
                    market.id,
                )
                total_par = self.rpc_client.get_market_total_par(
                    chain_code, margin_address, market.id
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="dolomite_market_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": market.symbol},
                    )
                )
                continue

            state = DolomiteMarketState(
                token_address=token_address,
                price_raw=price_raw,
                interest_rate_raw=interest_rate_raw,
                current_index=current_index,
                total_par=total_par,
            )

            price_usd = self._normalize_price(price_raw, market.decimals)
            total_borrow_raw = self._par_to_wei(total_par.borrow, current_index.borrow)
            total_supply_raw = self._par_to_wei(total_par.supply, current_index.supply)
            total_borrow_amount = normalize_raw_amount(total_borrow_raw, market.decimals)
            total_supply_amount = normalize_raw_amount(total_supply_raw, market.decimals)
            utilization = self._utilization(total_supply_amount, total_borrow_amount)

            borrow_rate_per_second = Decimal(interest_rate_raw) / WAD
            supply_rate_per_second = borrow_rate_per_second * utilization

            runtimes[market.id] = _DolomiteMarketRuntime(
                config=market,
                market_ref=market_ref,
                state=state,
                price_usd=price_usd,
                borrow_rate_per_second=borrow_rate_per_second,
                supply_rate_per_second=supply_rate_per_second,
                borrow_apy=_safe_apy_from_per_second(borrow_rate_per_second),
                supply_apy=_safe_apy_from_per_second(supply_rate_per_second),
            )

        return runtimes, issues, block_number_or_slot

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.dolomite.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)

            margin_address = canonical_address(chain_config.margin)
            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                for account_number in chain_config.account_numbers:
                    for market_id, runtime in runtimes.items():
                        try:
                            account_wei = self.rpc_client.get_account_wei(
                                chain_code,
                                margin_address,
                                wallet_address,
                                account_number,
                                market_id,
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="dolomite_account_read_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=runtime.market_ref,
                                    payload_json={
                                        "market_id": market_id,
                                        "account_number": account_number,
                                    },
                                )
                            )
                            continue

                        if account_wei.value == 0:
                            continue

                        supplied_raw = account_wei.value if account_wei.is_positive else 0
                        borrowed_raw = account_wei.value if not account_wei.is_positive else 0
                        supplied_amount = normalize_raw_amount(
                            supplied_raw, runtime.config.decimals
                        )
                        borrowed_amount = normalize_raw_amount(
                            borrowed_raw, runtime.config.decimals
                        )

                        price_usd = self._resolve_price_usd(
                            chain_code=chain_code,
                            runtime=runtime,
                            prices_by_token=prices_by_token,
                        )

                        supplied_usd = supplied_amount * price_usd
                        borrowed_usd = borrowed_amount * price_usd
                        equity_usd = supplied_usd - borrowed_usd
                        ltv = borrowed_usd / supplied_usd if supplied_usd > 0 else None
                        supply_apy = runtime.supply_apy
                        if (
                            supplied_amount > 0
                            and self.avant_yield_oracle is not None
                            and self._supports_avant_native_yield(runtime.config.symbol)
                        ):
                            try:
                                supply_apy = self.avant_yield_oracle.get_token_apy(
                                    runtime.config.symbol
                                )
                            except Exception as exc:
                                issues.append(
                                    self._issue(
                                        as_of_ts_utc=as_of_ts_utc,
                                        stage="sync_snapshot",
                                        error_type="dolomite_underlying_apy_fetch_failed",
                                        error_message=str(exc),
                                        chain_code=chain_code,
                                        wallet_address=wallet_address,
                                        market_ref=runtime.market_ref,
                                        payload_json={
                                            "symbol": runtime.config.symbol,
                                            "source": "avant_api",
                                        },
                                    )
                                )
                        elif (
                            supplied_amount > 0
                            and self.yield_oracle is not None
                            and runtime.config.defillama_pool_id is not None
                        ):
                            try:
                                supply_apy = self.yield_oracle.get_pool_apy(
                                    runtime.config.defillama_pool_id
                                )
                            except Exception as exc:
                                issues.append(
                                    self._issue(
                                        as_of_ts_utc=as_of_ts_utc,
                                        stage="sync_snapshot",
                                        error_type="dolomite_underlying_apy_fetch_failed",
                                        error_message=str(exc),
                                        chain_code=chain_code,
                                        wallet_address=wallet_address,
                                        market_ref=runtime.market_ref,
                                        payload_json={
                                            "symbol": runtime.config.symbol,
                                            "source": "defillama_pool",
                                            "pool_id": runtime.config.defillama_pool_id,
                                        },
                                    )
                                )

                        positions.append(
                            PositionSnapshotInput(
                                as_of_ts_utc=as_of_ts_utc,
                                protocol_code=self.protocol_code,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=runtime.market_ref,
                                position_key=self._position_key(
                                    chain_code,
                                    wallet_address,
                                    account_number,
                                    runtime.market_ref,
                                ),
                                supplied_amount=supplied_amount,
                                supplied_usd=supplied_usd,
                                borrowed_amount=borrowed_amount,
                                borrowed_usd=borrowed_usd,
                                supply_apy=supply_apy,
                                borrow_apy=runtime.borrow_apy,
                                reward_apy=Decimal("0"),
                                equity_usd=equity_usd,
                                ltv=ltv,
                                source="rpc",
                                block_number_or_slot=block_number_or_slot,
                            )
                        )

        return positions, issues

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        snapshots: list[MarketSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.dolomite.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)

            for _market_id, runtime in runtimes.items():
                total_borrow_raw = self._par_to_wei(
                    runtime.state.total_par.borrow,
                    runtime.state.current_index.borrow,
                )
                total_supply_raw = self._par_to_wei(
                    runtime.state.total_par.supply,
                    runtime.state.current_index.supply,
                )

                total_borrow_amount = normalize_raw_amount(
                    total_borrow_raw, runtime.config.decimals
                )
                total_supply_amount = normalize_raw_amount(
                    total_supply_raw, runtime.config.decimals
                )
                price_usd = self._resolve_price_usd(
                    chain_code=chain_code,
                    runtime=runtime,
                    prices_by_token=prices_by_token,
                )
                total_supply_usd = total_supply_amount * price_usd
                total_borrow_usd = total_borrow_amount * price_usd
                utilization = self._utilization(total_supply_usd, total_borrow_usd)
                available_liquidity_usd = max(total_supply_usd - total_borrow_usd, Decimal("0"))

                irm_params_json = {
                    "token_address": runtime.state.token_address,
                    "price_raw": str(runtime.state.price_raw),
                    "interest_rate_raw": str(runtime.state.interest_rate_raw),
                    "borrow_index": str(runtime.state.current_index.borrow),
                    "supply_index": str(runtime.state.current_index.supply),
                    "last_update": runtime.state.current_index.last_update,
                    "total_borrow_par": str(runtime.state.total_par.borrow),
                    "total_supply_par": str(runtime.state.total_par.supply),
                    "borrow_apy": str(runtime.borrow_apy),
                    "supply_apy": str(runtime.supply_apy),
                }

                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=runtime.market_ref,
                        total_supply_usd=total_supply_usd,
                        total_borrow_usd=total_borrow_usd,
                        utilization=utilization,
                        supply_apy=runtime.supply_apy,
                        borrow_apy=runtime.borrow_apy,
                        source="rpc",
                        block_number_or_slot=block_number_or_slot,
                        available_liquidity_usd=available_liquidity_usd,
                        caps_json=None,
                        irm_params_json=irm_params_json,
                    )
                )

        return snapshots, issues
