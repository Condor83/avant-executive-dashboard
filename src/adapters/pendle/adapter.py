"""Pendle adapter for canonical PT/YT position and market snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from core.config import MarketsConfig, PendleChainConfig, PendleMarket, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput

from .history import PendleMarketMetadata, PendleWalletPosition

ZERO = Decimal("0")
PENDLE_CHAIN_IDS = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
}


class PendleClient(Protocol):
    """Protocol for Pendle reads used by the adapter."""

    def get_markets(self, *, chain_id: int) -> list[PendleMarketMetadata]:
        """Return Pendle market metadata for one chain."""

    def get_wallet_positions(
        self, *, chain_id: int, wallet_address: str
    ) -> list[PendleWalletPosition]:
        """Return normalized PT/YT wallet balances for one chain."""


@dataclass(frozen=True)
class _ConfiguredPendleMarket:
    chain_code: str
    config: PendleMarket
    live: PendleMarketMetadata


class PendleAdapter:
    """Collect canonical Pendle PT/YT positions and market snapshots."""

    protocol_code = "pendle"

    def __init__(self, *, markets_config: MarketsConfig, client: PendleClient) -> None:
        self.markets_config = markets_config
        self.client = client

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str, leg: str) -> str:
        return f"pendle:{chain_code}:{wallet_address}:{market_ref}:{leg}"

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
    def _normalize_token_amount(raw_amount: Decimal, decimals: int) -> Decimal:
        return raw_amount / (Decimal(10) ** Decimal(decimals))

    @staticmethod
    def _yt_supply_apy(live_market: PendleMarketMetadata) -> Decimal | None:
        if live_market.underlying_apy is None or live_market.implied_apy is None:
            return None
        return live_market.underlying_apy - live_market.implied_apy

    def _chain_runtime(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        chain_code: str,
        chain_config: PendleChainConfig,
    ) -> tuple[dict[str, _ConfiguredPendleMarket], list[DataQualityIssue]]:
        chain_id = PENDLE_CHAIN_IDS.get(chain_code)
        if chain_id is None:
            return {}, [
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="pendle_chain_mapping_missing",
                    error_message="Pendle chain mapping is missing",
                    chain_code=chain_code,
                )
            ]

        try:
            live_markets = self.client.get_markets(chain_id=chain_id)
        except Exception as exc:
            return {}, [
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="pendle_market_fetch_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                )
            ]

        live_by_address = {market.market_address: market for market in live_markets}
        runtime: dict[str, _ConfiguredPendleMarket] = {}
        issues: list[DataQualityIssue] = []

        for market in chain_config.markets:
            market_ref = canonical_address(market.market_address)
            live = live_by_address.get(market_ref)
            if live is None:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="pendle_market_missing",
                        error_message="configured Pendle market not found in Pendle API response",
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"market_name": market.name},
                    )
                )
                continue

            expected_pt = canonical_address(market.pt_token.address)
            expected_yt = canonical_address(market.yt_token.address)
            if live.pt_token_address and expected_pt != live.pt_token_address:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="pendle_market_token_mismatch",
                        error_message=(
                            "configured PT token address does not match Pendle API market"
                        ),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"configured": expected_pt, "observed": live.pt_token_address},
                    )
                )
            if live.yt_token_address and expected_yt != live.yt_token_address:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="pendle_market_token_mismatch",
                        error_message=(
                            "configured YT token address does not match Pendle API market"
                        ),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"configured": expected_yt, "observed": live.yt_token_address},
                    )
                )

            runtime[market_ref] = _ConfiguredPendleMarket(
                chain_code=chain_code,
                config=market,
                live=live,
            )

        return runtime, issues

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        del prices_by_token
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        for chain_code, chain_config in self.markets_config.pendle.items():
            runtime, chain_issues = self._chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(chain_issues)
            if not runtime:
                continue

            chain_id = PENDLE_CHAIN_IDS.get(chain_code)
            if chain_id is None:
                continue

            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                try:
                    wallet_positions = self.client.get_wallet_positions(
                        chain_id=chain_id,
                        wallet_address=wallet_address,
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="pendle_wallet_positions_fetch_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                        )
                    )
                    continue

                wallet_positions_by_market = {
                    position.market_address: position for position in wallet_positions
                }

                for market_ref, configured in runtime.items():
                    wallet_position = wallet_positions_by_market.get(market_ref)
                    if wallet_position is None:
                        continue

                    pt_amount = self._normalize_token_amount(
                        wallet_position.pt_balance,
                        configured.config.pt_token.decimals,
                    )
                    pt_usd = wallet_position.pt_valuation_usd
                    if pt_amount > ZERO or pt_usd > ZERO:
                        positions.append(
                            PositionSnapshotInput(
                                as_of_ts_utc=as_of_ts_utc,
                                protocol_code=self.protocol_code,
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                position_key=self._position_key(
                                    chain_code, wallet_address, market_ref, "pt"
                                ),
                                supplied_amount=pt_amount,
                                supplied_usd=pt_usd,
                                borrowed_amount=ZERO,
                                borrowed_usd=ZERO,
                                supply_apy=ZERO,
                                borrow_apy=ZERO,
                                reward_apy=ZERO,
                                equity_usd=pt_usd,
                                source="rpc",
                            )
                        )

                    yt_amount = self._normalize_token_amount(
                        wallet_position.yt_balance,
                        configured.config.yt_token.decimals,
                    )
                    yt_usd = wallet_position.yt_valuation_usd
                    if yt_amount <= ZERO and yt_usd <= ZERO:
                        continue

                    yt_supply_apy = self._yt_supply_apy(configured.live)
                    if yt_supply_apy is None:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="pendle_yt_apy_unresolved",
                                error_message=(
                                    "Pendle market data did not include both underlying "
                                    "and implied APY"
                                ),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                            )
                        )
                        yt_supply_apy = ZERO

                    positions.append(
                        PositionSnapshotInput(
                            as_of_ts_utc=as_of_ts_utc,
                            protocol_code=self.protocol_code,
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            position_key=self._position_key(
                                chain_code, wallet_address, market_ref, "yt"
                            ),
                            supplied_amount=ZERO,
                            supplied_usd=ZERO,
                            borrowed_amount=ZERO,
                            borrowed_usd=ZERO,
                            supply_apy=yt_supply_apy,
                            borrow_apy=ZERO,
                            reward_apy=ZERO,
                            equity_usd=yt_usd,
                            source="rpc",
                            collateral_amount=yt_amount,
                            collateral_usd=yt_usd,
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

        for chain_code, chain_config in self.markets_config.pendle.items():
            runtime, chain_issues = self._chain_runtime(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(chain_issues)
            for market_ref, configured in runtime.items():
                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=market_ref,
                        total_supply_usd=configured.live.total_tvl_usd or ZERO,
                        total_borrow_usd=ZERO,
                        utilization=ZERO,
                        supply_apy=configured.live.implied_apy or ZERO,
                        borrow_apy=ZERO,
                        source="rpc",
                        available_liquidity_usd=configured.live.liquidity_usd or ZERO,
                        irm_params_json={
                            "name": configured.config.name,
                            "expiry": configured.config.expiry.isoformat(),
                            "underlying_apy": (
                                str(configured.live.underlying_apy)
                                if configured.live.underlying_apy is not None
                                else None
                            ),
                            "total_pt": (
                                str(configured.live.total_pt)
                                if configured.live.total_pt is not None
                                else None
                            ),
                            "total_sy": (
                                str(configured.live.total_sy)
                                if configured.live.total_sy is not None
                                else None
                            ),
                            "implied_apy": (
                                str(configured.live.implied_apy)
                                if configured.live.implied_apy is not None
                                else None
                            ),
                            "pendle_apy": (
                                str(configured.live.pendle_apy)
                                if configured.live.pendle_apy is not None
                                else None
                            ),
                            "swap_fee_apy": (
                                str(configured.live.swap_fee_apy)
                                if configured.live.swap_fee_apy is not None
                                else None
                            ),
                            "aggregated_apy": (
                                str(configured.live.aggregated_apy)
                                if configured.live.aggregated_apy is not None
                                else None
                            ),
                            "pt_token_address": configured.live.pt_token_address,
                            "yt_token_address": configured.live.yt_token_address,
                            "sy_token_address": configured.live.sy_token_address,
                            "underlying_token_address": configured.live.underlying_token_address,
                        },
                    )
                )

        return snapshots, issues
