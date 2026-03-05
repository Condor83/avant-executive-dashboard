"""Morpho Blue adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import (
    MarketsConfig,
    MorphoChainConfig,
    MorphoMarket,
    MorphoVault,
    canonical_address,
)
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput
from core.yields import DefiLlamaYieldOracle

MORPHO_POSITION_SELECTOR = "0x93c52062"
MORPHO_MARKET_SELECTOR = "0x5c60e39a"
MORPHO_MARKET_PARAMS_SELECTOR = "0x2c3c9157"
MORPHO_BORROW_RATE_VIEW_SELECTOR = "0x8c00bf6b"
ERC20_DECIMALS_SELECTOR = "0x313ce567"
ERC20_BALANCE_OF_SELECTOR = "0x70a08231"
ERC4626_ASSET_SELECTOR = "0x38d52e0f"
ERC4626_CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"

WAD = Decimal("1e18")
BPS = Decimal("1e4")
SECONDS_PER_YEAR = Decimal("31536000")
SECONDS_PER_YEAR_FLOAT = 31536000.0


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
    """Convert raw on-chain integer amounts into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _strip_0x_hex(value: str) -> str:
    cleaned = value.strip().lower()
    return cleaned[2:] if cleaned.startswith("0x") else cleaned


def _encode_bytes32(value: str) -> str:
    return _strip_0x_hex(value).rjust(64, "0")


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
class MorphoPosition:
    supply_shares: int
    borrow_shares: int
    collateral: int


@dataclass(frozen=True)
class MorphoMarketState:
    total_supply_assets: int
    total_supply_shares: int
    total_borrow_assets: int
    total_borrow_shares: int
    last_update: int
    fee: int


@dataclass(frozen=True)
class MorphoMarketParams:
    loan_token: str
    collateral_token: str
    oracle: str
    irm: str
    lltv: int


@dataclass(frozen=True)
class _MorphoMarketRuntime:
    config: MorphoMarket
    market_ref: str
    market_state: MorphoMarketState
    market_params: MorphoMarketParams
    loan_decimals: int
    collateral_decimals: int | None
    borrow_rate_per_second: Decimal
    supply_rate_per_second: Decimal
    borrow_apy: Decimal
    supply_apy: Decimal


@dataclass(frozen=True)
class _MorphoVaultRuntime:
    config: MorphoVault
    market_ref: str
    asset_address: str
    asset_symbol: str
    asset_decimals: int


class MorphoRpcClient(Protocol):
    """Protocol for Morpho Blue reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        """Return position shares/collateral for a user and market."""

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        """Return market totals/shares and fee."""

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        """Return market parameters for a market id."""

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        """Return per-second borrow rate in WAD units from IRM."""

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        """Return ERC20 decimals for a token."""

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        """Return ERC20 balance for wallet/token."""

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        """Return ERC4626 underlying asset address for a vault."""

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        """Convert ERC4626 vault shares into underlying assets."""


class EvmRpcMorphoClient:
    """JSON-RPC client for Morpho Blue calls against EVM chains."""

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

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        data = (
            f"{MORPHO_POSITION_SELECTOR}"
            f"{_encode_bytes32(market_id)}"
            f"{_encode_address(wallet_address)}"
        )
        words = self._eth_call_words(chain_code, morpho_address, data)
        if len(words) < 3:
            raise RuntimeError("Morpho position call returned insufficient words")
        return MorphoPosition(
            supply_shares=words[0],
            borrow_shares=words[1],
            collateral=words[2],
        )

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        data = f"{MORPHO_MARKET_SELECTOR}{_encode_bytes32(market_id)}"
        words = self._eth_call_words(chain_code, morpho_address, data)
        if len(words) < 6:
            raise RuntimeError("Morpho market call returned insufficient words")
        return MorphoMarketState(
            total_supply_assets=words[0],
            total_supply_shares=words[1],
            total_borrow_assets=words[2],
            total_borrow_shares=words[3],
            last_update=words[4],
            fee=words[5],
        )

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        data = f"{MORPHO_MARKET_PARAMS_SELECTOR}{_encode_bytes32(market_id)}"
        words = self._eth_call_words(chain_code, morpho_address, data)
        if len(words) < 5:
            raise RuntimeError("Morpho idToMarketParams call returned insufficient words")
        return MorphoMarketParams(
            loan_token=_decode_address_word(words[0]),
            collateral_token=_decode_address_word(words[1]),
            oracle=_decode_address_word(words[2]),
            irm=_decode_address_word(words[3]),
            lltv=words[4],
        )

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        data = MORPHO_BORROW_RATE_VIEW_SELECTOR
        data += _encode_address(market_params.loan_token)
        data += _encode_address(market_params.collateral_token)
        data += _encode_address(market_params.oracle)
        data += _encode_address(market_params.irm)
        data += _encode_uint(market_params.lltv)
        data += _encode_uint(market_state.total_supply_assets)
        data += _encode_uint(market_state.total_supply_shares)
        data += _encode_uint(market_state.total_borrow_assets)
        data += _encode_uint(market_state.total_borrow_shares)
        data += _encode_uint(market_state.last_update)
        data += _encode_uint(market_state.fee)

        words = self._eth_call_words(chain_code, market_params.irm, data)
        if not words:
            raise RuntimeError("Morpho IRM borrowRateView returned empty response")
        return words[0]

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        words = self._eth_call_words(chain_code, token_address, ERC20_DECIMALS_SELECTOR)
        if not words:
            raise RuntimeError("ERC20 decimals call returned empty response")
        return int(words[0])

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        data = f"{ERC20_BALANCE_OF_SELECTOR}{_encode_address(wallet_address)}"
        words = self._eth_call_words(chain_code, token_address, data)
        if not words:
            return 0
        return int(words[0])

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        words = self._eth_call_words(chain_code, vault_address, ERC4626_ASSET_SELECTOR)
        if not words:
            raise RuntimeError("ERC4626 asset() returned empty response")
        return _decode_address_word(words[0])

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        data = f"{ERC4626_CONVERT_TO_ASSETS_SELECTOR}{_encode_uint(shares)}"
        words = self._eth_call_words(chain_code, vault_address, data)
        if not words:
            raise RuntimeError("ERC4626 convertToAssets() returned empty response")
        return int(words[0])


class MorphoAdapter:
    """Collect canonical Morpho Blue positions and market snapshots."""

    protocol_code = "morpho"

    def __init__(
        self,
        markets_config: MarketsConfig,
        rpc_client: MorphoRpcClient,
        *,
        defillama_timeout_seconds: float = 15.0,
        yield_oracle: DefiLlamaYieldOracle | None = None,
    ) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client
        self.yield_oracle = yield_oracle or DefiLlamaYieldOracle(
            timeout_seconds=defillama_timeout_seconds
        )

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str) -> str:
        return f"morpho:{chain_code}:{wallet_address}:{market_ref}"

    @staticmethod
    def _safe_share_to_assets(shares: int, total_assets: int, total_shares: int) -> int:
        if shares <= 0 or total_assets <= 0 or total_shares <= 0:
            return 0
        return (shares * total_assets) // total_shares

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

    @staticmethod
    def _normalize_ltv_from_wad(raw_ltv_wad: int) -> Decimal:
        return Decimal(raw_ltv_wad) / WAD

    @staticmethod
    def _price_from_map(
        prices_by_token: dict[tuple[str, str], Decimal],
        *,
        chain_code: str,
        token_address: str | None,
        symbol: str,
    ) -> Decimal | None:
        if token_address:
            address_price = prices_by_token.get((chain_code, canonical_address(token_address)))
            if address_price is not None:
                return address_price
        return prices_by_token.get((chain_code, f"symbol:{symbol.strip().upper()}"))

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

    def _collect_chain_market_runtime(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        chain_code: str,
        chain_config: MorphoChainConfig,
    ) -> tuple[dict[str, _MorphoMarketRuntime], list[DataQualityIssue], str | None]:
        runtimes: dict[str, _MorphoMarketRuntime] = {}
        issues: list[DataQualityIssue] = []

        block_number_or_slot: str | None = None
        try:
            block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="morpho_block_number_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                )
            )

        morpho_address = canonical_address(chain_config.morpho)
        for market in chain_config.markets:
            market_ref = canonical_address(market.id)
            try:
                market_state = self.rpc_client.get_market_state(
                    chain_code, morpho_address, market_ref
                )
                market_params = self.rpc_client.get_market_params(
                    chain_code, morpho_address, market_ref
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="morpho_market_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={
                            "loan_token": market.loan_token,
                            "collateral_token": market.collateral_token,
                        },
                    )
                )
                continue

            collateral_decimals = market.collateral_decimals
            if collateral_decimals is None:
                try:
                    collateral_decimals = self.rpc_client.get_erc20_decimals(
                        chain_code,
                        market_params.collateral_token,
                    )
                except Exception:
                    collateral_decimals = None

            borrow_rate_raw = 0
            try:
                borrow_rate_raw = self.rpc_client.get_irm_borrow_rate(
                    chain_code,
                    market_params,
                    market_state,
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="morpho_rate_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                    )
                )

            borrow_rate_per_second = Decimal(borrow_rate_raw) / WAD
            utilization = self._utilization(
                Decimal(market_state.total_supply_assets),
                Decimal(market_state.total_borrow_assets),
            )
            fee_rate = Decimal(market_state.fee) / WAD
            if fee_rate < 0:
                fee_rate = Decimal("0")
            if fee_rate > 1:
                fee_rate = Decimal("1")
            supply_rate_per_second = (
                borrow_rate_per_second * utilization * (Decimal("1") - fee_rate)
            )

            runtimes[market_ref] = _MorphoMarketRuntime(
                config=market,
                market_ref=market_ref,
                market_state=market_state,
                market_params=market_params,
                loan_decimals=market.loan_decimals,
                collateral_decimals=collateral_decimals,
                borrow_rate_per_second=borrow_rate_per_second,
                supply_rate_per_second=supply_rate_per_second,
                borrow_apy=_safe_apy_from_per_second(borrow_rate_per_second),
                supply_apy=_safe_apy_from_per_second(supply_rate_per_second),
            )

        return runtimes, issues, block_number_or_slot

    def _collect_chain_vault_runtime(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        chain_config: MorphoChainConfig,
    ) -> tuple[dict[str, _MorphoVaultRuntime], list[DataQualityIssue]]:
        runtimes: dict[str, _MorphoVaultRuntime] = {}
        issues: list[DataQualityIssue] = []

        for vault in chain_config.vaults:
            market_ref = canonical_address(vault.address)

            asset_address = (
                canonical_address(vault.asset_address) if vault.asset_address is not None else None
            )
            if asset_address is None:
                try:
                    asset_address = canonical_address(
                        self.rpc_client.get_vault_asset(chain_code, market_ref)
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="morpho_vault_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"field": "asset_address"},
                        )
                    )
                    continue

            asset_decimals = vault.asset_decimals
            if asset_decimals is None:
                try:
                    asset_decimals = self.rpc_client.get_erc20_decimals(chain_code, asset_address)
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="morpho_vault_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "field": "asset_decimals",
                                "asset_address": asset_address,
                            },
                        )
                    )
                    continue

            asset_symbol = (vault.asset_symbol or "").strip().upper() or "UNKNOWN"

            runtimes[market_ref] = _MorphoVaultRuntime(
                config=vault,
                market_ref=market_ref,
                asset_address=asset_address,
                asset_symbol=asset_symbol,
                asset_decimals=int(asset_decimals),
            )

        return runtimes, issues

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.morpho.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_market_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)
            vault_runtimes, vault_issues = self._collect_chain_vault_runtime(
                as_of_ts_utc=as_of_ts_utc,
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(vault_issues)

            morpho_address = canonical_address(chain_config.morpho)
            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                for market_ref, runtime in runtimes.items():
                    try:
                        position = self.rpc_client.get_position(
                            chain_code,
                            morpho_address,
                            market_ref,
                            wallet_address,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="morpho_position_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                            )
                        )
                        continue

                    supplied_raw = self._safe_share_to_assets(
                        position.supply_shares,
                        runtime.market_state.total_supply_assets,
                        runtime.market_state.total_supply_shares,
                    )
                    borrowed_raw = self._safe_share_to_assets(
                        position.borrow_shares,
                        runtime.market_state.total_borrow_assets,
                        runtime.market_state.total_borrow_shares,
                    )

                    collateral_amount = Decimal("0")
                    if runtime.collateral_decimals is not None:
                        collateral_amount = normalize_raw_amount(
                            position.collateral,
                            runtime.collateral_decimals,
                        )
                    elif position.collateral > 0:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="collateral_decimals_missing",
                                error_message=(
                                    "collateral decimals unavailable; collateral USD omitted"
                                ),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"collateral_raw": str(position.collateral)},
                            )
                        )

                    supplied_amount = normalize_raw_amount(supplied_raw, runtime.loan_decimals)
                    borrowed_amount = normalize_raw_amount(borrowed_raw, runtime.loan_decimals)

                    if supplied_amount == 0 and borrowed_amount == 0 and collateral_amount == 0:
                        continue

                    loan_price = self._price_from_map(
                        prices_by_token,
                        chain_code=chain_code,
                        token_address=runtime.market_params.loan_token,
                        symbol=runtime.config.loan_token,
                    )
                    collateral_price = self._price_from_map(
                        prices_by_token,
                        chain_code=chain_code,
                        token_address=runtime.market_params.collateral_token,
                        symbol=runtime.config.collateral_token,
                    )

                    if loan_price is None:
                        loan_price = Decimal("0")
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="price_missing",
                                error_message="no price available for Morpho loan token",
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": runtime.config.loan_token},
                            )
                        )
                    if collateral_price is None:
                        collateral_price = Decimal("0")
                        if collateral_amount > 0:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="price_missing",
                                    error_message="no price available for Morpho collateral token",
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=market_ref,
                                    payload_json={"symbol": runtime.config.collateral_token},
                                )
                            )

                    supplied_usd = (supplied_amount * loan_price) + (
                        collateral_amount * collateral_price
                    )
                    borrowed_usd = borrowed_amount * loan_price
                    equity_usd = supplied_usd - borrowed_usd
                    ltv = borrowed_usd / supplied_usd if supplied_usd > 0 else None
                    supply_apy = runtime.supply_apy
                    if runtime.config.defillama_pool_id and collateral_amount > 0:
                        try:
                            supply_apy = self.yield_oracle.get_pool_apy(
                                runtime.config.defillama_pool_id
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="morpho_collateral_apy_fallback_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    wallet_address=wallet_address,
                                    market_ref=market_ref,
                                    payload_json={
                                        "pool_id": runtime.config.defillama_pool_id,
                                        "collateral_token": runtime.config.collateral_token,
                                    },
                                )
                            )

                    positions.append(
                        PositionSnapshotInput(
                            as_of_ts_utc=as_of_ts_utc,
                            protocol_code=self.protocol_code,
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            position_key=self._position_key(chain_code, wallet_address, market_ref),
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

                for market_ref, vault_runtime in vault_runtimes.items():
                    try:
                        shares = self.rpc_client.get_erc20_balance(
                            chain_code,
                            market_ref,
                            wallet_address,
                        )
                        assets_raw = (
                            self.rpc_client.convert_to_assets(chain_code, market_ref, shares)
                            if shares > 0
                            else 0
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="morpho_vault_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"field": "balance_or_convert_to_assets"},
                            )
                        )
                        continue

                    if assets_raw <= 0:
                        continue

                    supplied_amount = normalize_raw_amount(assets_raw, vault_runtime.asset_decimals)
                    if supplied_amount <= 0:
                        continue

                    asset_price = self._price_from_map(
                        prices_by_token,
                        chain_code=chain_code,
                        token_address=vault_runtime.asset_address,
                        symbol=vault_runtime.asset_symbol,
                    )
                    if asset_price is None:
                        asset_price = Decimal("0")
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="price_missing",
                                error_message=(
                                    "no price available for Morpho vault underlying token"
                                ),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={
                                    "symbol": vault_runtime.asset_symbol,
                                    "asset_address": vault_runtime.asset_address,
                                },
                            )
                        )

                    supplied_usd = supplied_amount * asset_price

                    positions.append(
                        PositionSnapshotInput(
                            as_of_ts_utc=as_of_ts_utc,
                            protocol_code=self.protocol_code,
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            position_key=self._position_key(chain_code, wallet_address, market_ref),
                            supplied_amount=supplied_amount,
                            supplied_usd=supplied_usd,
                            borrowed_amount=Decimal("0"),
                            borrowed_usd=Decimal("0"),
                            supply_apy=Decimal("0"),
                            borrow_apy=Decimal("0"),
                            reward_apy=Decimal("0"),
                            equity_usd=supplied_usd,
                            ltv=None,
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

        for chain_code, chain_config in self.markets_config.morpho.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_market_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)

            for market_ref, runtime in runtimes.items():
                loan_price = self._price_from_map(
                    prices_by_token,
                    chain_code=chain_code,
                    token_address=runtime.market_params.loan_token,
                    symbol=runtime.config.loan_token,
                )
                if loan_price is None:
                    loan_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Morpho loan token",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": runtime.config.loan_token},
                        )
                    )

                total_supply_amount = normalize_raw_amount(
                    runtime.market_state.total_supply_assets,
                    runtime.loan_decimals,
                )
                total_borrow_amount = normalize_raw_amount(
                    runtime.market_state.total_borrow_assets,
                    runtime.loan_decimals,
                )

                total_supply_usd = total_supply_amount * loan_price
                total_borrow_usd = total_borrow_amount * loan_price
                utilization = self._utilization(total_supply_usd, total_borrow_usd)
                available_liquidity_usd = max(total_supply_usd - total_borrow_usd, Decimal("0"))

                irm_params_json = {
                    "loan_token": runtime.market_params.loan_token,
                    "collateral_token": runtime.market_params.collateral_token,
                    "oracle": runtime.market_params.oracle,
                    "irm": runtime.market_params.irm,
                    "lltv": str(runtime.market_params.lltv),
                    "fee": str(runtime.market_state.fee),
                    "borrow_rate_per_second": str(runtime.borrow_rate_per_second),
                    "supply_rate_per_second": str(runtime.supply_rate_per_second),
                    "borrow_apy": str(runtime.borrow_apy),
                    "supply_apy": str(runtime.supply_apy),
                    "defillama_pool_id": runtime.config.defillama_pool_id,
                    "position_supply_apy_policy": (
                        "collateral_carry_from_defillama_pool_when_configured"
                    ),
                    "protocol_supply_apy": str(runtime.supply_apy),
                }

                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=market_ref,
                        total_supply_usd=total_supply_usd,
                        total_borrow_usd=total_borrow_usd,
                        utilization=utilization,
                        supply_apy=runtime.supply_apy,
                        borrow_apy=runtime.borrow_apy,
                        source="rpc",
                        block_number_or_slot=block_number_or_slot,
                        available_liquidity_usd=available_liquidity_usd,
                        max_ltv=self._normalize_ltv_from_wad(runtime.market_params.lltv),
                        caps_json=None,
                        irm_params_json=irm_params_json,
                    )
                )

        return snapshots, issues
