"""Euler v2 adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import httpx

from core.config import EulerChainConfig, EulerVault, MarketsConfig, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput

ERC20_DECIMALS_SELECTOR = "0x313ce567"
ERC20_BALANCE_OF_SELECTOR = "0x70a08231"
EULER_ASSET_SELECTOR = "0x38d52e0f"
EULER_TOTAL_ASSETS_SELECTOR = "0x01e1d114"
EULER_TOTAL_BORROWS_SELECTOR = "0x47bd3718"
EULER_INTEREST_RATE_SELECTOR = "0x7c3a00fd"
EULER_INTEREST_FEE_SELECTOR = "0xa75df498"
EULER_CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"
EULER_DEBT_OF_SELECTOR = "0xd283e75f"

RAY = Decimal("1e27")
BPS = Decimal("1e4")
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
class EulerVaultState:
    asset_address: str
    asset_decimals: int
    total_assets: int
    total_borrows: int
    interest_rate: int
    interest_fee: int | None


@dataclass(frozen=True)
class _EulerVaultRuntime:
    config: EulerVault
    market_ref: str
    state: EulerVaultState
    debt_supported: bool
    borrow_rate_per_second: Decimal
    supply_rate_per_second: Decimal
    borrow_apy: Decimal
    supply_apy: Decimal


@dataclass(frozen=True)
class _EulerAccountExposure:
    market_ref: str
    supplied_amount: Decimal
    supplied_usd: Decimal
    borrowed_amount: Decimal
    borrowed_usd: Decimal
    runtime: _EulerVaultRuntime


class EulerV2RpcClient(Protocol):
    """Protocol for Euler v2 reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        """Return the underlying asset address for a vault."""

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        """Return ERC20 decimals for a token."""

    def get_total_assets(self, chain_code: str, vault_address: str) -> int:
        """Return total supplied assets for a vault."""

    def get_total_borrows(self, chain_code: str, vault_address: str) -> int:
        """Return total borrowed assets for a vault."""

    def get_interest_rate(self, chain_code: str, vault_address: str) -> int:
        """Return borrow rate-per-second in WAD units."""

    def get_interest_fee(self, chain_code: str, vault_address: str) -> int | None:
        """Return protocol fee (bps) if available."""

    def get_balance_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        """Return vault-share balance for a wallet."""

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        """Convert vault shares into underlying asset amount."""

    def get_debt_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        """Return wallet debt in underlying asset units."""


class EvmRpcEulerV2Client:
    """JSON-RPC client for Euler v2 calls against EVM chains."""

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

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        words = self._eth_call_words(chain_code, vault_address, EULER_ASSET_SELECTOR)
        if not words:
            raise RuntimeError("Euler asset() returned empty response")
        return _decode_address_word(words[0])

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        words = self._eth_call_words(chain_code, token_address, ERC20_DECIMALS_SELECTOR)
        if not words:
            raise RuntimeError("ERC20 decimals call returned empty response")
        return int(words[0])

    def get_total_assets(self, chain_code: str, vault_address: str) -> int:
        words = self._eth_call_words(chain_code, vault_address, EULER_TOTAL_ASSETS_SELECTOR)
        if not words:
            raise RuntimeError("Euler totalAssets() returned empty response")
        return words[0]

    def get_total_borrows(self, chain_code: str, vault_address: str) -> int:
        words = self._eth_call_words(chain_code, vault_address, EULER_TOTAL_BORROWS_SELECTOR)
        if not words:
            raise RuntimeError("Euler totalBorrows() returned empty response")
        return words[0]

    def get_interest_rate(self, chain_code: str, vault_address: str) -> int:
        words = self._eth_call_words(chain_code, vault_address, EULER_INTEREST_RATE_SELECTOR)
        if not words:
            raise RuntimeError("Euler interestRate() returned empty response")
        return words[0]

    def get_interest_fee(self, chain_code: str, vault_address: str) -> int | None:
        try:
            words = self._eth_call_words(chain_code, vault_address, EULER_INTEREST_FEE_SELECTOR)
        except Exception:
            return None
        if not words:
            return None
        return int(words[0])

    def get_balance_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        data = f"{ERC20_BALANCE_OF_SELECTOR}{_encode_address(wallet_address)}"
        words = self._eth_call_words(chain_code, vault_address, data)
        if not words:
            raise RuntimeError("Euler balanceOf() returned empty response")
        return words[0]

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        data = f"{EULER_CONVERT_TO_ASSETS_SELECTOR}{_encode_uint(shares)}"
        words = self._eth_call_words(chain_code, vault_address, data)
        if not words:
            raise RuntimeError("Euler convertToAssets() returned empty response")
        return words[0]

    def get_debt_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        data = f"{EULER_DEBT_OF_SELECTOR}{_encode_address(wallet_address)}"
        words = self._eth_call_words(chain_code, vault_address, data)
        if not words:
            raise RuntimeError("Euler debtOf() returned empty response")
        return words[0]


class EulerV2Adapter:
    """Collect canonical Euler v2 positions and market snapshots."""

    protocol_code = "euler_v2"

    def __init__(self, markets_config: MarketsConfig, rpc_client: EulerV2RpcClient) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str) -> str:
        return f"euler_v2:{chain_code}:{wallet_address}:{market_ref}"

    @classmethod
    def _position_key_for_account(
        cls,
        *,
        chain_code: str,
        wallet_address: str,
        market_ref: str,
        account_id: int,
    ) -> str:
        base_key = cls._position_key(chain_code, wallet_address, market_ref)
        if account_id == 0:
            return base_key
        return f"{base_key}:acct{account_id}"

    @staticmethod
    def _subaccount_address(wallet_address: str, account_id: int) -> str:
        """Derive Euler subaccount address (owner XOR account_id)."""

        if account_id < 0:
            raise ValueError("account_id must be non-negative")
        owner = int(canonical_address(wallet_address), 16)
        return f"0x{(owner ^ account_id):040x}"

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

    @staticmethod
    def _symbol_candidates(vault_symbol: str) -> list[str]:
        base = vault_symbol.strip()
        candidates = [base]
        if base.lower().startswith("e") and len(base) > 1:
            candidates.append(base[1:])
        return list(dict.fromkeys(candidates))

    @classmethod
    def _price_from_map(
        cls,
        prices_by_token: dict[tuple[str, str], Decimal],
        *,
        chain_code: str,
        token_address: str,
        asset_symbol: str,
        vault_symbol: str,
    ) -> Decimal | None:
        address_price = prices_by_token.get((chain_code, canonical_address(token_address)))
        if address_price is not None:
            return address_price

        for symbol in cls._symbol_candidates(asset_symbol):
            symbol_price = prices_by_token.get((chain_code, f"symbol:{symbol.upper()}"))
            if symbol_price is not None:
                return symbol_price

        for symbol in cls._symbol_candidates(vault_symbol):
            symbol_price = prices_by_token.get((chain_code, f"symbol:{symbol.upper()}"))
            if symbol_price is not None:
                return symbol_price
        return None

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

    def _build_position_input(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        wallet_address: str,
        account_id: int,
        market_ref: str,
        block_number_or_slot: str | None,
        supplied_amount: Decimal,
        supplied_usd: Decimal,
        borrowed_amount: Decimal,
        borrowed_usd: Decimal,
        supply_apy: Decimal,
        borrow_apy: Decimal,
    ) -> PositionSnapshotInput:
        equity_usd = supplied_usd - borrowed_usd
        ltv = borrowed_usd / supplied_usd if supplied_usd > 0 else None
        return PositionSnapshotInput(
            as_of_ts_utc=as_of_ts_utc,
            protocol_code=self.protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
            market_ref=market_ref,
            position_key=self._position_key_for_account(
                chain_code=chain_code,
                wallet_address=wallet_address,
                market_ref=market_ref,
                account_id=account_id,
            ),
            supplied_amount=supplied_amount,
            supplied_usd=supplied_usd,
            borrowed_amount=borrowed_amount,
            borrowed_usd=borrowed_usd,
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            reward_apy=Decimal("0"),
            equity_usd=equity_usd,
            ltv=ltv,
            source="rpc",
            block_number_or_slot=block_number_or_slot,
        )

    def _collect_wallet_account_exposures(
        self,
        *,
        as_of_ts_utc: datetime,
        chain_code: str,
        wallet_address: str,
        subaccount_address: str,
        runtimes: dict[str, _EulerVaultRuntime],
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[_EulerAccountExposure], list[DataQualityIssue]]:
        exposures: list[_EulerAccountExposure] = []
        issues: list[DataQualityIssue] = []

        for market_ref, runtime in runtimes.items():
            try:
                shares = self.rpc_client.get_balance_of(chain_code, market_ref, subaccount_address)
                supplied_raw = self.rpc_client.convert_to_assets(chain_code, market_ref, shares)
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="euler_position_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        wallet_address=wallet_address,
                        market_ref=market_ref,
                        payload_json={
                            "symbol": runtime.config.symbol,
                            "subaccount": subaccount_address,
                        },
                    )
                )
                continue

            borrowed_raw = 0
            if runtime.debt_supported:
                try:
                    borrowed_raw = self.rpc_client.get_debt_of(
                        chain_code, market_ref, subaccount_address
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="euler_debt_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            payload_json={
                                "symbol": runtime.config.symbol,
                                "subaccount": subaccount_address,
                            },
                        )
                    )

            supplied_amount = normalize_raw_amount(supplied_raw, runtime.state.asset_decimals)
            borrowed_amount = normalize_raw_amount(borrowed_raw, runtime.state.asset_decimals)
            if supplied_amount == 0 and borrowed_amount == 0:
                continue

            asset_price = self._price_from_map(
                prices_by_token,
                chain_code=chain_code,
                token_address=runtime.config.asset_address,
                asset_symbol=runtime.config.asset_symbol,
                vault_symbol=runtime.config.symbol,
            )
            if asset_price is None:
                asset_price = Decimal("0")
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="price_missing",
                        error_message="no price available for Euler vault underlying token",
                        chain_code=chain_code,
                        wallet_address=wallet_address,
                        market_ref=market_ref,
                        payload_json={
                            "symbol": runtime.config.symbol,
                            "asset": canonical_address(runtime.config.asset_address),
                            "subaccount": subaccount_address,
                        },
                    )
                )

            supplied_usd = supplied_amount * asset_price
            borrowed_usd = borrowed_amount * asset_price
            exposures.append(
                _EulerAccountExposure(
                    market_ref=market_ref,
                    supplied_amount=supplied_amount,
                    supplied_usd=supplied_usd,
                    borrowed_amount=borrowed_amount,
                    borrowed_usd=borrowed_usd,
                    runtime=runtime,
                )
            )

        return exposures, issues

    def _collect_chain_runtime(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        chain_code: str,
        chain_config: EulerChainConfig,
    ) -> tuple[dict[str, _EulerVaultRuntime], list[DataQualityIssue], str | None]:
        runtimes: dict[str, _EulerVaultRuntime] = {}
        issues: list[DataQualityIssue] = []

        block_number_or_slot: str | None = None
        try:
            block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="euler_block_number_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                )
            )

        for vault in chain_config.vaults:
            market_ref = canonical_address(vault.address)
            try:
                observed_asset_address = canonical_address(
                    self.rpc_client.get_vault_asset(chain_code, market_ref)
                )
                observed_asset_decimals = self.rpc_client.get_erc20_decimals(
                    chain_code, observed_asset_address
                )
                total_assets = self.rpc_client.get_total_assets(chain_code, market_ref)
                interest_fee = self.rpc_client.get_interest_fee(chain_code, market_ref)
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="euler_vault_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": vault.symbol},
                    )
                )
                continue

            debt_supported = True
            try:
                total_borrows = self.rpc_client.get_total_borrows(chain_code, market_ref)
            except Exception as exc:
                debt_supported = False
                total_borrows = 0
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="euler_total_borrows_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": vault.symbol},
                    )
                )

            interest_rate = 0
            if debt_supported:
                try:
                    interest_rate = self.rpc_client.get_interest_rate(chain_code, market_ref)
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage=stage,
                            error_type="euler_interest_rate_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": vault.symbol},
                        )
                    )

            configured_asset_address = canonical_address(vault.asset_address)
            configured_asset_decimals = int(vault.asset_decimals)
            if (
                configured_asset_address != observed_asset_address
                or configured_asset_decimals != observed_asset_decimals
            ):
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="euler_asset_mismatch",
                        error_message=(
                            "Euler vault asset metadata differs between config and on-chain reads"
                        ),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={
                            "symbol": vault.symbol,
                            "configured_asset_address": configured_asset_address,
                            "configured_asset_decimals": configured_asset_decimals,
                            "observed_asset_address": observed_asset_address,
                            "observed_asset_decimals": observed_asset_decimals,
                        },
                    )
                )

            state = EulerVaultState(
                asset_address=observed_asset_address,
                asset_decimals=observed_asset_decimals,
                total_assets=total_assets,
                total_borrows=total_borrows,
                interest_rate=interest_rate,
                interest_fee=interest_fee,
            )

            total_supply_amount = normalize_raw_amount(total_assets, observed_asset_decimals)
            total_borrow_amount = normalize_raw_amount(total_borrows, observed_asset_decimals)
            utilization = self._utilization(total_supply_amount, total_borrow_amount)

            # Euler v2 rates are ray-scaled per-second values.
            borrow_rate_per_second = Decimal(interest_rate) / RAY
            fee_rate = Decimal("0")
            if interest_fee is not None:
                fee_rate = Decimal(interest_fee) / BPS
                if fee_rate < 0:
                    fee_rate = Decimal("0")
                if fee_rate > 1:
                    fee_rate = Decimal("1")

            supply_rate_per_second = (
                borrow_rate_per_second * utilization * (Decimal("1") - fee_rate)
            )

            runtimes[market_ref] = _EulerVaultRuntime(
                config=vault,
                market_ref=market_ref,
                state=state,
                debt_supported=debt_supported,
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

        for chain_code, chain_config in self.markets_config.euler_v2.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)

            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                for account_id in list(dict.fromkeys(chain_config.account_ids)):
                    subaccount_address = self._subaccount_address(wallet_address, account_id)
                    account_exposures, account_issues = self._collect_wallet_account_exposures(
                        as_of_ts_utc=as_of_ts_utc,
                        chain_code=chain_code,
                        wallet_address=wallet_address,
                        subaccount_address=subaccount_address,
                        runtimes=runtimes,
                        prices_by_token=prices_by_token,
                    )
                    issues.extend(account_issues)
                    if not account_exposures:
                        continue

                    supply_exposures = [
                        exposure
                        for exposure in account_exposures
                        if exposure.supplied_amount > 0 and exposure.supplied_usd > 0
                    ]
                    borrow_exposures = [
                        exposure
                        for exposure in account_exposures
                        if exposure.borrowed_amount > 0 and exposure.borrowed_usd > 0
                    ]

                    can_synthesize_market = (
                        len(supply_exposures) == 1
                        and len(borrow_exposures) == 1
                        and supply_exposures[0].market_ref != borrow_exposures[0].market_ref
                    )
                    if can_synthesize_market:
                        supply_exposure = supply_exposures[0]
                        borrow_exposure = borrow_exposures[0]
                        synthetic_market_ref = (
                            f"{supply_exposure.market_ref}/{borrow_exposure.market_ref}"
                        )
                        positions.append(
                            self._build_position_input(
                                as_of_ts_utc=as_of_ts_utc,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                account_id=account_id,
                                market_ref=synthetic_market_ref,
                                block_number_or_slot=block_number_or_slot,
                                supplied_amount=supply_exposure.supplied_amount,
                                supplied_usd=supply_exposure.supplied_usd,
                                borrowed_amount=borrow_exposure.borrowed_amount,
                                borrowed_usd=borrow_exposure.borrowed_usd,
                                supply_apy=supply_exposure.runtime.supply_apy,
                                borrow_apy=borrow_exposure.runtime.borrow_apy,
                            )
                        )
                        continue

                    has_supply_and_borrow = bool(supply_exposures and borrow_exposures)
                    should_flag_ambiguous_subaccount = (
                        account_id != 0
                        and has_supply_and_borrow
                        and (len(supply_exposures) > 1 or len(borrow_exposures) > 1)
                    )
                    if should_flag_ambiguous_subaccount:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="euler_subaccount_pairing_ambiguous",
                                error_message=(
                                    "unable to reduce subaccount exposures to one supply/borrow "
                                    "pair"
                                ),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                payload_json={
                                    "account_id": account_id,
                                    "subaccount": subaccount_address,
                                    "supply_markets": sorted(
                                        {exposure.market_ref for exposure in supply_exposures}
                                    ),
                                    "borrow_markets": sorted(
                                        {exposure.market_ref for exposure in borrow_exposures}
                                    ),
                                },
                            )
                        )

                    for exposure in account_exposures:
                        positions.append(
                            self._build_position_input(
                                as_of_ts_utc=as_of_ts_utc,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                account_id=account_id,
                                market_ref=exposure.market_ref,
                                block_number_or_slot=block_number_or_slot,
                                supplied_amount=exposure.supplied_amount,
                                supplied_usd=exposure.supplied_usd,
                                borrowed_amount=exposure.borrowed_amount,
                                borrowed_usd=exposure.borrowed_usd,
                                supply_apy=exposure.runtime.supply_apy,
                                borrow_apy=exposure.runtime.borrow_apy,
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

        for chain_code, chain_config in self.markets_config.euler_v2.items():
            runtimes, runtime_issues, block_number_or_slot = self._collect_chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(runtime_issues)

            for market_ref, runtime in runtimes.items():
                asset_price = self._price_from_map(
                    prices_by_token,
                    chain_code=chain_code,
                    token_address=runtime.config.asset_address,
                    asset_symbol=runtime.config.asset_symbol,
                    vault_symbol=runtime.config.symbol,
                )
                if asset_price is None:
                    asset_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Euler vault underlying token",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "symbol": runtime.config.symbol,
                                "asset": canonical_address(runtime.config.asset_address),
                            },
                        )
                    )

                total_supply_amount = normalize_raw_amount(
                    runtime.state.total_assets,
                    runtime.state.asset_decimals,
                )
                total_borrow_amount = normalize_raw_amount(
                    runtime.state.total_borrows,
                    runtime.state.asset_decimals,
                )
                total_supply_usd = total_supply_amount * asset_price
                total_borrow_usd = total_borrow_amount * asset_price
                utilization = self._utilization(total_supply_usd, total_borrow_usd)
                available_liquidity_usd = max(total_supply_usd - total_borrow_usd, Decimal("0"))

                irm_params_json = {
                    "asset": runtime.state.asset_address,
                    "asset_decimals": runtime.state.asset_decimals,
                    "interest_rate_per_second": str(runtime.borrow_rate_per_second),
                    "interest_fee": (
                        str(runtime.state.interest_fee)
                        if runtime.state.interest_fee is not None
                        else None
                    ),
                    "borrow_apy": str(runtime.borrow_apy),
                    "supply_apy": str(runtime.supply_apy),
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
                        caps_json=None,
                        irm_params_json=irm_params_json,
                    )
                )

        return snapshots, issues
