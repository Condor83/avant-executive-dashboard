"""Kamino adapter for canonical market snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

import httpx

from core.config import MarketsConfig
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput


def _decimal_from(value: object, *, default: Decimal | None = None) -> Decimal:
    if value is None:
        if default is None:
            raise ValueError("missing decimal value")
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        if default is None:
            raise ValueError(f"invalid decimal value: {value}") from None
        return default


def normalize_rate_to_unit(value: object) -> Decimal:
    """Normalize APR/APY payloads to canonical 0.0-1.0 units."""

    rate = _decimal_from(value, default=Decimal("0"))
    if rate < 0:
        raise ValueError(f"rate cannot be negative: {rate}")
    if rate > Decimal("1"):
        return rate / Decimal("100")
    return rate


@dataclass(frozen=True)
class KaminoReserveRate:
    """Normalized reserve APYs and totals keyed by reserve account."""

    reserve_ref: str
    liquidity_token: str | None
    liquidity_token_mint: str | None
    supply_apy: Decimal
    borrow_apy: Decimal
    total_supply_usd: Decimal
    total_borrow_usd: Decimal


@dataclass(frozen=True)
class KaminoMarketStats:
    """Normalized Kamino market state used by the adapter."""

    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    utilization: Decimal | None = None
    available_liquidity_usd: Decimal | None = None
    slot: str | None = None
    raw_payload: dict[str, object] | None = None
    reserve_rates: dict[str, KaminoReserveRate] = field(default_factory=dict)


@dataclass(frozen=True)
class KaminoObligationSnapshot:
    """Normalized user-obligation snapshot for Kamino wallets."""

    obligation_ref: str
    supplied_usd: Decimal
    borrowed_usd: Decimal
    health_factor: Decimal | None = None
    ltv: Decimal | None = None
    block_number_or_slot: str | None = None
    deposit_reserve_values: dict[str, Decimal] = field(default_factory=dict)
    borrow_reserve_values: dict[str, Decimal] = field(default_factory=dict)


class KaminoClient(Protocol):
    """Protocol for Kamino market reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_market_stats(self, chain_code: str, market_pubkey: str) -> KaminoMarketStats:
        """Return normalized market-level metrics for a Kamino market."""

    def get_user_obligations(
        self,
        *,
        chain_code: str,
        market_pubkey: str,
        wallet_address: str,
    ) -> list[KaminoObligationSnapshot]:
        """Return normalized user obligations for a market/wallet pair."""


class KaminoYieldOracle(Protocol):
    """Protocol for external yield fallback reads."""

    def get_pool_apy(self, pool_id: str) -> Decimal:
        """Return latest APY in canonical 0.0-1.0 units."""


class KaminoApiClient:
    """HTTP client for Kamino market-state API reads."""

    def __init__(self, base_url: str, timeout_seconds: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _get_json(self, path: str) -> object:
        response = self._client.get(f"{self.base_url}{path}")
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _candidate_dicts(payload: object) -> list[dict[str, object]]:
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Kamino payload: {payload}")
        candidates = [payload]
        for key in ("data", "market", "result", "state", "metrics"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
        return candidates

    @classmethod
    def _extract_first(cls, payload: object, keys: tuple[str, ...]) -> object:
        for candidate in cls._candidate_dicts(payload):
            for key in keys:
                if key in candidate:
                    return candidate[key]
        return None

    def get_market_stats(self, chain_code: str, market_pubkey: str) -> KaminoMarketStats:
        del chain_code

        payload = self._get_json(f"/kamino-market/{market_pubkey}/reserves/metrics")
        if not isinstance(payload, list):
            raise RuntimeError(f"unexpected Kamino reserve metrics payload: {payload}")

        reserve_rows = [row for row in payload if isinstance(row, dict)]
        if not reserve_rows:
            raise RuntimeError("Kamino reserve metrics payload is empty")

        total_supply_usd = Decimal("0")
        total_borrow_usd = Decimal("0")
        supply_apy_weighted = Decimal("0")
        borrow_apy_weighted = Decimal("0")
        reserve_rates: dict[str, KaminoReserveRate] = {}

        for row in reserve_rows:
            supply_usd = _decimal_from(row.get("totalSupplyUsd"), default=Decimal("0"))
            borrow_usd = _decimal_from(row.get("totalBorrowUsd"), default=Decimal("0"))
            supply_apy = normalize_rate_to_unit(row.get("supplyApy", 0))
            borrow_apy = normalize_rate_to_unit(row.get("borrowApy", 0))
            reserve_ref_raw = row.get("reserve")
            reserve_ref = reserve_ref_raw if isinstance(reserve_ref_raw, str) else None

            total_supply_usd += supply_usd
            total_borrow_usd += borrow_usd
            supply_apy_weighted += supply_apy * supply_usd
            borrow_apy_weighted += borrow_apy * borrow_usd

            if reserve_ref:
                reserve_rates[reserve_ref] = KaminoReserveRate(
                    reserve_ref=reserve_ref,
                    liquidity_token=(
                        row.get("liquidityToken")
                        if isinstance(row.get("liquidityToken"), str)
                        else None
                    ),
                    liquidity_token_mint=(
                        row.get("liquidityTokenMint")
                        if isinstance(row.get("liquidityTokenMint"), str)
                        else None
                    ),
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    total_supply_usd=supply_usd,
                    total_borrow_usd=borrow_usd,
                )

        normalized_supply_apy = (
            supply_apy_weighted / total_supply_usd if total_supply_usd > 0 else Decimal("0")
        )
        normalized_borrow_apy = (
            borrow_apy_weighted / total_borrow_usd if total_borrow_usd > 0 else Decimal("0")
        )
        utilization = total_borrow_usd / total_supply_usd if total_supply_usd > 0 else Decimal("0")
        available_liquidity_usd = max(total_supply_usd - total_borrow_usd, Decimal("0"))

        metrics_history = self._get_json(f"/kamino-market/{market_pubkey}/metrics/history")
        slot: str | None = None
        if isinstance(metrics_history, list) and metrics_history:
            last_row = metrics_history[-1]
            if isinstance(last_row, dict):
                timestamp = last_row.get("timestamp")
                if timestamp is not None:
                    slot = str(timestamp)

        return KaminoMarketStats(
            total_supply_usd=total_supply_usd,
            total_borrow_usd=total_borrow_usd,
            supply_apy=normalized_supply_apy,
            borrow_apy=normalized_borrow_apy,
            utilization=utilization,
            available_liquidity_usd=available_liquidity_usd,
            slot=slot,
            raw_payload={"reserve_count": len(reserve_rows)},
            reserve_rates=reserve_rates,
        )

    @staticmethod
    def _extract_obligation_ref(payload: dict[str, object]) -> str | None:
        for key in ("obligation", "obligationAddress", "pubkey", "address"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    @staticmethod
    def _extract_decimal(payload: dict[str, object], keys: tuple[str, ...]) -> Decimal | None:
        for key in keys:
            if key in payload:
                try:
                    return _decimal_from(payload[key])
                except Exception:
                    return None
        return None

    @staticmethod
    def _extract_reserve_value_map(
        entries: object,
        *,
        reserve_key: str,
    ) -> dict[str, Decimal]:
        if not isinstance(entries, list):
            return {}

        reserve_values: dict[str, Decimal] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            reserve_ref = entry.get(reserve_key)
            if not isinstance(reserve_ref, str) or not reserve_ref:
                continue
            try:
                market_value = _decimal_from(entry.get("marketValueSf"), default=Decimal("0"))
            except Exception:
                continue
            if market_value <= 0:
                continue
            reserve_values[reserve_ref] = market_value
        return reserve_values

    @staticmethod
    def _extract_obligation_slot(row: dict[str, object]) -> str | None:
        timestamp = row.get("timestamp")
        if timestamp is not None:
            return str(timestamp)

        state = row.get("state")
        if not isinstance(state, dict):
            return None
        last_update = state.get("lastUpdate")
        if isinstance(last_update, dict):
            slot = last_update.get("slot")
            if slot is not None:
                return str(slot)
            stale = last_update.get("stale")
            if stale is not None:
                return str(stale)
        return None

    def get_user_obligations(
        self,
        *,
        chain_code: str,
        market_pubkey: str,
        wallet_address: str,
    ) -> list[KaminoObligationSnapshot]:
        del chain_code
        payload = self._get_json(
            f"/kamino-market/{market_pubkey}/users/{wallet_address}/obligations"
        )
        if not isinstance(payload, list):
            return []

        snapshots: list[KaminoObligationSnapshot] = []
        for row in payload:
            if not isinstance(row, dict):
                continue

            obligation_ref = self._extract_obligation_ref(row)
            if obligation_ref is None:
                continue

            refreshed_stats = row.get("refreshedStats")
            stats = refreshed_stats if isinstance(refreshed_stats, dict) else {}
            sol_values_raw = row.get("obligationSolValues")
            sol_values = sol_values_raw if isinstance(sol_values_raw, dict) else {}
            state_raw = row.get("state")
            state = state_raw if isinstance(state_raw, dict) else {}

            supplied_usd = self._extract_decimal(
                stats,
                ("userTotalDeposit", "totalDepositUsd", "depositUsd"),
            )
            borrowed_usd = self._extract_decimal(
                stats,
                ("userTotalBorrow", "totalBorrowUsd", "borrowUsd"),
            )

            if supplied_usd is None or borrowed_usd is None:
                collateral_sol = self._extract_decimal(sol_values, ("collateralValueSol",))
                debt_sol = self._extract_decimal(sol_values, ("debtValueSol",))
                sol_price = self._extract_decimal(sol_values, ("solPrice",))
                if collateral_sol is not None and debt_sol is not None and sol_price is not None:
                    supplied_usd = collateral_sol * sol_price
                    borrowed_usd = debt_sol * sol_price
                else:
                    supplied_usd = supplied_usd or Decimal("0")
                    borrowed_usd = borrowed_usd or Decimal("0")

            ltv = self._extract_decimal(stats, ("loanToValue",))
            health_factor = self._extract_decimal(stats, ("borrowLimit", "healthFactor"))
            block_number_or_slot = self._extract_obligation_slot(row)

            deposits_entries = state.get("deposits")
            if not isinstance(deposits_entries, list):
                deposits_entries = row.get("deposits")
            borrows_entries = state.get("borrows")
            if not isinstance(borrows_entries, list):
                borrows_entries = row.get("borrows")

            snapshots.append(
                KaminoObligationSnapshot(
                    obligation_ref=obligation_ref,
                    supplied_usd=max(supplied_usd, Decimal("0")),
                    borrowed_usd=max(borrowed_usd, Decimal("0")),
                    health_factor=health_factor,
                    ltv=ltv,
                    block_number_or_slot=block_number_or_slot,
                    deposit_reserve_values=self._extract_reserve_value_map(
                        deposits_entries,
                        reserve_key="depositReserve",
                    ),
                    borrow_reserve_values=self._extract_reserve_value_map(
                        borrows_entries,
                        reserve_key="borrowReserve",
                    ),
                )
            )

        return snapshots


class KaminoAdapter:
    """Collect canonical Kamino market snapshots."""

    protocol_code = "kamino"

    def __init__(
        self,
        markets_config: MarketsConfig,
        client: KaminoClient,
        yield_oracle: KaminoYieldOracle | None = None,
    ) -> None:
        self.markets_config = markets_config
        self.client = client
        self.yield_oracle = yield_oracle

    @staticmethod
    def _utilization(total_supply_usd: Decimal, total_borrow_usd: Decimal) -> Decimal:
        if total_supply_usd <= 0:
            return Decimal("0")
        return total_borrow_usd / total_supply_usd

    @staticmethod
    def _position_key(
        chain_code: str,
        wallet_address: str,
        market_ref: str,
        obligation_ref: str,
    ) -> str:
        return f"kamino:{chain_code}:{wallet_address}:{market_ref}:{obligation_ref}"

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

    @staticmethod
    def _weighted_reserve_apy(
        *,
        reserve_values: dict[str, Decimal],
        reserve_rates: dict[str, KaminoReserveRate],
        apy_kind: str,
        fallback_supply_apy: Decimal | None = None,
    ) -> Decimal | None:
        total_weight = Decimal("0")
        weighted_sum = Decimal("0")

        for reserve_ref, weight in reserve_values.items():
            if weight <= 0:
                continue
            rate_row = reserve_rates.get(reserve_ref)
            if rate_row is None:
                continue
            apy = rate_row.supply_apy if apy_kind == "supply" else rate_row.borrow_apy
            if apy_kind == "supply" and apy <= 0 and fallback_supply_apy is not None:
                apy = fallback_supply_apy
            weighted_sum += apy * weight
            total_weight += weight

        if total_weight <= 0:
            return None
        return weighted_sum / total_weight

    @staticmethod
    def _non_zero_reserve_symbols(
        reserve_values: dict[str, Decimal],
        reserve_rates: dict[str, KaminoReserveRate],
    ) -> set[str]:
        symbols: set[str] = set()
        for reserve_ref, weight in reserve_values.items():
            if weight <= 0:
                continue
            rate_row = reserve_rates.get(reserve_ref)
            if rate_row is None or not rate_row.liquidity_token:
                continue
            symbols.add(rate_row.liquidity_token)
        return symbols

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        del prices_by_token
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.kamino.items():
            for market in chain_config.markets:
                market_ref = market.market_pubkey
                market_rates: KaminoMarketStats | None = None
                try:
                    market_rates = self.client.get_market_stats(chain_code, market_ref)
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="kamino_market_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"market_name": market.name},
                        )
                    )
                    continue

                fallback_supply_apy: Decimal | None = None
                if market.defillama_pool_id and self.yield_oracle is not None:
                    try:
                        fallback_supply_apy = self.yield_oracle.get_pool_apy(
                            market.defillama_pool_id
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="kamino_supply_apy_fallback_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                market_ref=market_ref,
                                payload_json={
                                    "market_name": market.name,
                                    "pool_id": market.defillama_pool_id,
                                },
                            )
                        )

                for wallet in chain_config.wallets:
                    try:
                        obligations = self.client.get_user_obligations(
                            chain_code=chain_code,
                            market_pubkey=market_ref,
                            wallet_address=wallet,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="kamino_user_obligations_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet,
                                market_ref=market_ref,
                            )
                        )
                        continue

                    for obligation in obligations:
                        if obligation.supplied_usd == 0 and obligation.borrowed_usd == 0:
                            continue

                        observed_supply_symbols = self._non_zero_reserve_symbols(
                            obligation.deposit_reserve_values,
                            market_rates.reserve_rates,
                        )
                        observed_borrow_symbols = self._non_zero_reserve_symbols(
                            obligation.borrow_reserve_values,
                            market_rates.reserve_rates,
                        )
                        if market.supply_token is not None and len(observed_supply_symbols) > 1:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="kamino_multi_supply_token",
                                    error_message=(
                                        "obligation has multiple non-zero Kamino supply tokens"
                                    ),
                                    chain_code=chain_code,
                                    wallet_address=wallet,
                                    market_ref=market_ref,
                                    payload_json={
                                        "obligation_ref": obligation.obligation_ref,
                                        "expected_supply_token": market.supply_token.symbol,
                                        "observed_supply_tokens": sorted(observed_supply_symbols),
                                    },
                                )
                            )
                        if (
                            market.supply_token is not None
                            and observed_supply_symbols
                            and market.supply_token.symbol not in observed_supply_symbols
                        ):
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="kamino_supply_token_mismatch",
                                    error_message=(
                                        "configured Kamino supply token differs from obligation"
                                    ),
                                    chain_code=chain_code,
                                    wallet_address=wallet,
                                    market_ref=market_ref,
                                    payload_json={
                                        "obligation_ref": obligation.obligation_ref,
                                        "expected_supply_token": market.supply_token.symbol,
                                        "observed_supply_tokens": sorted(observed_supply_symbols),
                                    },
                                )
                            )
                        if market.borrow_token is not None and len(observed_borrow_symbols) > 1:
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="kamino_multi_borrow_token",
                                    error_message=(
                                        "obligation has multiple non-zero Kamino borrow tokens"
                                    ),
                                    chain_code=chain_code,
                                    wallet_address=wallet,
                                    market_ref=market_ref,
                                    payload_json={
                                        "obligation_ref": obligation.obligation_ref,
                                        "expected_borrow_token": market.borrow_token.symbol,
                                        "observed_borrow_tokens": sorted(observed_borrow_symbols),
                                    },
                                )
                            )
                        if (
                            market.borrow_token is not None
                            and observed_borrow_symbols
                            and market.borrow_token.symbol not in observed_borrow_symbols
                        ):
                            issues.append(
                                self._issue(
                                    as_of_ts_utc=as_of_ts_utc,
                                    stage="sync_snapshot",
                                    error_type="kamino_borrow_token_mismatch",
                                    error_message=(
                                        "configured Kamino borrow token differs from obligation"
                                    ),
                                    chain_code=chain_code,
                                    wallet_address=wallet,
                                    market_ref=market_ref,
                                    payload_json={
                                        "obligation_ref": obligation.obligation_ref,
                                        "expected_borrow_token": market.borrow_token.symbol,
                                        "observed_borrow_tokens": sorted(observed_borrow_symbols),
                                    },
                                )
                            )

                        # Kamino obligations can span multiple reserve assets. For canonical market
                        # rows we store aggregated notional values in amount fields.
                        supplied_amount = obligation.supplied_usd
                        borrowed_amount = obligation.borrowed_usd
                        position_supply_apy = self._weighted_reserve_apy(
                            reserve_values=obligation.deposit_reserve_values,
                            reserve_rates=market_rates.reserve_rates,
                            apy_kind="supply",
                            fallback_supply_apy=fallback_supply_apy,
                        )
                        position_borrow_apy = self._weighted_reserve_apy(
                            reserve_values=obligation.borrow_reserve_values,
                            reserve_rates=market_rates.reserve_rates,
                            apy_kind="borrow",
                        )

                        if position_supply_apy is None:
                            position_supply_apy = market_rates.supply_apy
                            if position_supply_apy <= 0 and fallback_supply_apy is not None:
                                position_supply_apy = fallback_supply_apy

                        if position_borrow_apy is None:
                            position_borrow_apy = market_rates.borrow_apy

                        positions.append(
                            PositionSnapshotInput(
                                as_of_ts_utc=as_of_ts_utc,
                                protocol_code=self.protocol_code,
                                chain_code=chain_code,
                                wallet_address=wallet,
                                market_ref=market_ref,
                                position_key=self._position_key(
                                    chain_code,
                                    wallet,
                                    market_ref,
                                    obligation.obligation_ref,
                                ),
                                supplied_amount=supplied_amount,
                                supplied_usd=obligation.supplied_usd,
                                borrowed_amount=borrowed_amount,
                                borrowed_usd=obligation.borrowed_usd,
                                supply_apy=max(position_supply_apy, Decimal("0")),
                                borrow_apy=max(position_borrow_apy, Decimal("0")),
                                reward_apy=Decimal("0"),
                                equity_usd=obligation.supplied_usd - obligation.borrowed_usd,
                                source="rpc",
                                block_number_or_slot=(
                                    obligation.block_number_or_slot or market_rates.slot
                                ),
                                health_factor=obligation.health_factor,
                                ltv=obligation.ltv,
                            )
                        )

        return positions, issues

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        del prices_by_token
        snapshots: list[MarketSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.kamino.items():
            for market in chain_config.markets:
                market_ref = market.market_pubkey
                try:
                    stats = self.client.get_market_stats(chain_code, market_ref)
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="kamino_market_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"market_name": market.name},
                        )
                    )
                    continue

                total_supply_usd = max(stats.total_supply_usd, Decimal("0"))
                total_borrow_usd = max(stats.total_borrow_usd, Decimal("0"))
                utilization = stats.utilization
                if utilization is None:
                    utilization = self._utilization(total_supply_usd, total_borrow_usd)

                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=market_ref,
                        total_supply_usd=total_supply_usd,
                        total_borrow_usd=total_borrow_usd,
                        utilization=utilization,
                        supply_apy=max(stats.supply_apy, Decimal("0")),
                        borrow_apy=max(stats.borrow_apy, Decimal("0")),
                        source="rpc",
                        block_number_or_slot=stats.slot,
                        available_liquidity_usd=stats.available_liquidity_usd,
                        irm_params_json={
                            "market_name": market.name,
                            "raw_payload_keys": sorted(stats.raw_payload.keys())
                            if isinstance(stats.raw_payload, dict)
                            else None,
                        },
                    )
                )

        return snapshots, issues
