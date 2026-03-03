"""Zest adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from core.config import MarketsConfig
from core.stacks_client import StacksClient
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput

INDEX_SCALE = 100_000_000
RATE_SCALE = Decimal("100000000")


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
    """Convert raw token balances into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _to_decimal(value: object, *, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _to_int(value: object) -> int | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if parsed != parsed.to_integral_value():
        return None
    return int(parsed)


@dataclass(frozen=True)
class ZestMarketTotalsRaw:
    """Raw on-chain (integer) market totals for a Zest market."""

    total_supply_raw: int
    total_borrow_raw: int


@dataclass(frozen=True)
class ZestMarketRates:
    """Normalized 0.0-1.0 APY pair for a Zest market."""

    supply_apy: Decimal
    borrow_apy: Decimal


class ZestClient(Protocol):
    """Protocol for Zest reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_height(self, chain_code: str) -> int:
        """Return latest block height for the configured Stacks chain."""

    def get_wallet_supply_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        asset_contract: str,
        z_token_identifier: str,
        wallet_address: str,
    ) -> int:
        """Return raw supplied amount for a wallet market leg."""

    def get_wallet_borrow_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        borrow_fn: str,
        wallet_address: str,
        asset_contract: str,
    ) -> int:
        """Return raw borrowed amount for a wallet market leg."""

    def get_market_totals_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
        z_token_identifier: str,
    ) -> ZestMarketTotalsRaw | None:
        """Return chain-wide raw market totals if available."""

    def get_market_rates(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
    ) -> ZestMarketRates | None:
        """Return normalized market rates if available."""


class StacksApiZestClient:
    """Stacks-backed Zest client using Hiro read-only contract calls."""

    def __init__(
        self,
        *,
        stacks_client: StacksClient,
        zest_api_base_url: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.stacks_client = stacks_client
        self.zest_api_base_url = zest_api_base_url
        # Kept for backward compatibility with existing CLI wiring.
        self.timeout_seconds = timeout_seconds

    def close(self) -> None:
        """Close transport resources."""

        return

    def get_block_height(self, chain_code: str) -> int:
        del chain_code
        return self.stacks_client.get_block_height()

    def get_wallet_supply_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        asset_contract: str,
        z_token_identifier: str,
        wallet_address: str,
    ) -> int:
        del chain_code
        shares_raw = self.stacks_client.get_ft_balance(
            wallet_address,
            z_token_identifier,
        ).balance_raw
        if shares_raw <= 0:
            return 0

        reserve = self._get_reserve_data(
            pool_deployer=pool_deployer,
            pool_read=pool_read,
            asset_contract=asset_contract,
        )
        liquidity_index = INDEX_SCALE
        if reserve is not None:
            parsed_index = self._extract_int(
                reserve,
                ("last-liquidity-cumulative-index", "liquidity-index"),
            )
            if parsed_index is not None and parsed_index > 0:
                liquidity_index = parsed_index
        underlying_raw = (shares_raw * liquidity_index) // INDEX_SCALE
        return max(underlying_raw, 0)

    @staticmethod
    def _extract_int(
        payload: dict[str, object],
        keys: tuple[str, ...],
    ) -> int | None:
        for key in keys:
            if key not in payload:
                continue
            parsed = _to_int(payload[key])
            if parsed is not None:
                return parsed
        return None

    def _get_reserve_data(
        self,
        *,
        pool_deployer: str,
        pool_read: str,
        asset_contract: str,
    ) -> dict[str, object] | None:
        asset_hex = self.stacks_client.serialize_contract_principal(asset_contract)
        result = self.stacks_client.call_read_only(
            contract_address=pool_deployer,
            contract_name=pool_read,
            function_name="get-reserve-data",
            arguments=[asset_hex],
            sender=pool_deployer,
        )
        if not isinstance(result, dict):
            return None
        return result

    def get_wallet_borrow_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        borrow_fn: str,
        wallet_address: str,
        asset_contract: str,
    ) -> int:
        del chain_code, asset_contract
        principal_hex = self.stacks_client.serialize_principal(wallet_address)

        try:
            payload = self.stacks_client.call_read_only(
                contract_address=pool_deployer,
                contract_name=pool_read,
                function_name=borrow_fn,
                arguments=[principal_hex],
                sender=wallet_address,
            )
        except Exception as exc:
            if "UnwrapFailure" in str(exc):
                return 0
            raise

        if isinstance(payload, int):
            return max(payload, 0)
        if not isinstance(payload, dict):
            return 0
        raw_balance = self._extract_int(
            payload,
            (
                "compounded-balance",
                "compounded_balance",
                "borrowRaw",
                "totalBorrowRaw",
            ),
        )
        if raw_balance is None:
            return 0
        return max(raw_balance, 0)

    def get_market_totals_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
        z_token_identifier: str,
    ) -> ZestMarketTotalsRaw | None:
        del chain_code, market_symbol

        reserve = self._get_reserve_data(
            pool_deployer=pool_deployer,
            pool_read=pool_read,
            asset_contract=asset_contract,
        )
        if reserve is None:
            return None

        total_borrow_raw = self._extract_int(
            reserve,
            ("total-borrows-variable", "totalBorrowRaw"),
        )
        if total_borrow_raw is None:
            return None

        liquidity_index = self._extract_int(
            reserve,
            ("last-liquidity-cumulative-index", "liquidity-index"),
        )
        if liquidity_index is None or liquidity_index <= 0:
            liquidity_index = INDEX_SCALE

        token_contract_id = z_token_identifier.split("::", 1)[0]
        if "." not in token_contract_id:
            return None
        token_deployer, token_contract = token_contract_id.split(".", 1)

        total_supply_shares = self.stacks_client.call_read_only(
            contract_address=token_deployer,
            contract_name=token_contract,
            function_name="get-total-supply",
            arguments=[],
            sender=pool_deployer,
        )
        shares_raw = _to_int(total_supply_shares)
        if shares_raw is None:
            return None
        total_supply_raw = (shares_raw * liquidity_index) // INDEX_SCALE

        return ZestMarketTotalsRaw(
            total_supply_raw=max(total_supply_raw, 0),
            total_borrow_raw=max(total_borrow_raw, 0),
        )

    def get_market_rates(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
    ) -> ZestMarketRates | None:
        del chain_code, market_symbol
        reserve = self._get_reserve_data(
            pool_deployer=pool_deployer,
            pool_read=pool_read,
            asset_contract=asset_contract,
        )
        if reserve is None:
            return None

        supply_rate_raw = _to_decimal(
            reserve.get("current-liquidity-rate"),
            default=Decimal("0"),
        )
        borrow_rate_raw = _to_decimal(
            reserve.get("current-variable-borrow-rate"),
            default=Decimal("0"),
        )
        return ZestMarketRates(
            supply_apy=max(supply_rate_raw / RATE_SCALE, Decimal("0")),
            borrow_apy=max(borrow_rate_raw / RATE_SCALE, Decimal("0")),
        )


class ZestAdapter:
    """Collect canonical Zest positions and market snapshots."""

    protocol_code = "zest"

    def __init__(self, markets_config: MarketsConfig, client: ZestClient) -> None:
        self.markets_config = markets_config
        self.client = client

    @staticmethod
    def _trim_error_message(error_message: str, *, limit: int = 1000) -> str:
        if len(error_message) <= limit:
            return error_message
        return f"{error_message[:limit]}...[truncated]"

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str) -> str:
        return f"zest:{chain_code}:{wallet_address}:{market_ref}"

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

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
            error_message=self._trim_error_message(error_message),
            protocol_code=self.protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
            market_ref=market_ref,
            payload_json=payload_json,
        )

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.zest.items():
            block_number_or_slot: str | None = None
            try:
                block_number_or_slot = str(self.client.get_block_height(chain_code))
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="zest_block_height_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

            for wallet_address in chain_config.wallets:
                for market in chain_config.markets:
                    market_ref = market.asset_contract

                    try:
                        supplied_raw = self.client.get_wallet_supply_raw(
                            chain_code=chain_code,
                            pool_deployer=chain_config.pool_deployer,
                            pool_read=chain_config.pool_read,
                            asset_contract=market_ref,
                            z_token_identifier=market.z_token,
                            wallet_address=wallet_address,
                        )
                        borrowed_raw = self.client.get_wallet_borrow_raw(
                            chain_code=chain_code,
                            pool_deployer=chain_config.pool_deployer,
                            pool_read=chain_config.pool_read,
                            borrow_fn=market.borrow_fn,
                            wallet_address=wallet_address,
                            asset_contract=market_ref,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="zest_wallet_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": market.symbol},
                            )
                        )
                        continue

                    supplied_amount = normalize_raw_amount(supplied_raw, market.decimals)
                    borrowed_amount = normalize_raw_amount(borrowed_raw, market.decimals)
                    if supplied_amount == 0 and borrowed_amount == 0:
                        continue

                    price_key = (chain_code, market_ref)
                    price_usd = prices_by_token.get(price_key)
                    if price_usd is None:
                        price_usd = Decimal("0")
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="price_missing",
                                error_message="no price available for Zest asset",
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": market.symbol},
                            )
                        )

                    supplied_usd = supplied_amount * price_usd
                    borrowed_usd = borrowed_amount * price_usd
                    equity_usd = supplied_usd - borrowed_usd

                    try:
                        rates = self.client.get_market_rates(
                            chain_code=chain_code,
                            pool_deployer=chain_config.pool_deployer,
                            pool_read=chain_config.pool_read,
                            market_symbol=market.symbol,
                            asset_contract=market_ref,
                        )
                        supply_apy = rates.supply_apy if rates is not None else Decimal("0")
                        borrow_apy = rates.borrow_apy if rates is not None else Decimal("0")
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="zest_market_rates_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": market.symbol},
                            )
                        )
                        supply_apy = Decimal("0")
                        borrow_apy = Decimal("0")

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
                            borrow_apy=borrow_apy,
                            reward_apy=Decimal("0"),
                            equity_usd=equity_usd,
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

        for chain_code, chain_config in self.markets_config.zest.items():
            block_number_or_slot: str | None = None
            try:
                block_number_or_slot = str(self.client.get_block_height(chain_code))
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_markets",
                        error_type="zest_block_height_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

            for market in chain_config.markets:
                market_ref = market.asset_contract

                try:
                    rates = self.client.get_market_rates(
                        chain_code=chain_code,
                        pool_deployer=chain_config.pool_deployer,
                        pool_read=chain_config.pool_read,
                        market_symbol=market.symbol,
                        asset_contract=market_ref,
                    )
                    supply_apy = rates.supply_apy if rates is not None else Decimal("0")
                    borrow_apy = rates.borrow_apy if rates is not None else Decimal("0")
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="zest_market_rates_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.symbol},
                        )
                    )
                    supply_apy = Decimal("0")
                    borrow_apy = Decimal("0")

                try:
                    totals = self.client.get_market_totals_raw(
                        chain_code=chain_code,
                        pool_deployer=chain_config.pool_deployer,
                        pool_read=chain_config.pool_read,
                        market_symbol=market.symbol,
                        asset_contract=market_ref,
                        z_token_identifier=market.z_token,
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="zest_market_totals_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.symbol},
                        )
                    )
                    totals = None

                if totals is None:
                    sum_supply_raw = 0
                    sum_borrow_raw = 0
                    for wallet_address in chain_config.wallets:
                        try:
                            sum_supply_raw += self.client.get_wallet_supply_raw(
                                chain_code=chain_code,
                                pool_deployer=chain_config.pool_deployer,
                                pool_read=chain_config.pool_read,
                                asset_contract=market_ref,
                                z_token_identifier=market.z_token,
                                wallet_address=wallet_address,
                            )
                            sum_borrow_raw += self.client.get_wallet_borrow_raw(
                                chain_code=chain_code,
                                pool_deployer=chain_config.pool_deployer,
                                pool_read=chain_config.pool_read,
                                borrow_fn=market.borrow_fn,
                                wallet_address=wallet_address,
                                asset_contract=market_ref,
                            )
                        except Exception as exc:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_markets",
                                    error_type="zest_market_total_fallback_failed",
                                    error_message=str(exc),
                                    chain_code=chain_code,
                                    market_ref=market_ref,
                                    payload_json={
                                        "symbol": market.symbol,
                                        "wallet": wallet_address,
                                    },
                                )
                            )
                    totals = ZestMarketTotalsRaw(
                        total_supply_raw=sum_supply_raw,
                        total_borrow_raw=sum_borrow_raw,
                    )

                total_supply_amount = normalize_raw_amount(totals.total_supply_raw, market.decimals)
                total_borrow_amount = normalize_raw_amount(totals.total_borrow_raw, market.decimals)

                price_key = (chain_code, market_ref)
                price_usd = prices_by_token.get(price_key)
                if price_usd is None:
                    price_usd = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Zest asset",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.symbol},
                        )
                    )

                total_supply_usd = total_supply_amount * price_usd
                total_borrow_usd = total_borrow_amount * price_usd
                utilization = self._utilization(total_supply_amount, total_borrow_amount)

                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=market_ref,
                        total_supply_usd=total_supply_usd,
                        total_borrow_usd=total_borrow_usd,
                        utilization=utilization,
                        supply_apy=supply_apy,
                        borrow_apy=borrow_apy,
                        source="rpc",
                        block_number_or_slot=block_number_or_slot,
                        available_liquidity_usd=max(
                            total_supply_usd - total_borrow_usd, Decimal("0")
                        ),
                        irm_params_json={
                            "pool_deployer": chain_config.pool_deployer,
                            "pool_read": chain_config.pool_read,
                            "borrow_fn": market.borrow_fn,
                            "symbol": market.symbol,
                        },
                    )
                )

        return snapshots, issues
