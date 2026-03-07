"""Snapshot ingestion runner and persistence helpers."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import Select, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.market_exposures import ensure_market_exposures
from core.config import MarketsConfig, PTFixedYieldOverride
from core.dashboard_contracts import position_exposure_class
from core.db.models import (
    Chain,
    DataQuality,
    Market,
    MarketExposureComponent,
    MarketSnapshot,
    Position,
    PositionFixedYieldCache,
    PositionSnapshot,
    PositionSnapshotLeg,
    Price,
    Token,
    Wallet,
    WalletProductMap,
)
from core.db.models import Protocol as ProtocolModel
from core.position_contracts import (
    economic_supply_amount,
    economic_supply_token_id,
    economic_supply_usd,
)
from core.pricing import PriceOracle
from core.types import (
    DataQualityIssue,
    MarketSnapshotInput,
    PositionSnapshotInput,
    PriceQuote,
    PriceRequest,
)

DAYS_PER_YEAR = Decimal("365")
ZERO = Decimal("0")
PT_BALANCE_GROWTH_THRESHOLD = Decimal("1.01")
PENDLE_CHAIN_IDS = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
}


class PositionAdapter(Protocol):
    """Adapter contract for position snapshot collection."""

    protocol_code: str

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        """Collect canonical position snapshot rows and data-quality issues."""


class MarketAdapter(Protocol):
    """Adapter contract for market snapshot collection."""

    protocol_code: str

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        """Collect canonical market snapshot rows and data-quality issues."""


class PendleHistoryClientLike(Protocol):
    """Structural interface for Pendle history readers used in PT reconstruction."""

    def get_market_addresses_for_pt(self, *, chain_id: int, pt_token_address: str) -> set[str]:
        """Return Pendle market addresses associated with a PT token."""

    def get_wallet_trades(self, *, chain_id: int, wallet_address: str) -> list[Any]:
        """Return normalized wallet trade history for Pendle PT reconstruction."""


@dataclass(frozen=True)
class RunnerSummary:
    """Generic runner summary used by CLI output."""

    rows_written: int
    issues_written: int
    component_failures: int


@dataclass(frozen=True)
class _MarketDetail:
    market_id: int
    protocol_id: int
    protocol_code: str
    chain_id: int
    chain_code: str
    market_kind: str
    display_name: str
    base_asset_token_id: int | None
    base_asset_address: str | None
    base_asset_symbol: str | None
    collateral_token_id: int | None
    collateral_token_address: str | None
    collateral_symbol: str | None
    metadata_json: dict[str, Any] | None


class SnapshotRunner:
    """Coordinates pricing, adapters, and DB persistence for sync commands."""

    def __init__(
        self,
        *,
        session: Session,
        markets_config: MarketsConfig,
        price_oracle: PriceOracle | None,
        position_adapters: list[PositionAdapter],
        market_adapters: list[MarketAdapter] | None = None,
        pendle_history_client: PendleHistoryClientLike | None = None,
        pt_fixed_yield_overrides: dict[str, PTFixedYieldOverride] | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.session = session
        self.markets_config = markets_config
        self.price_oracle = price_oracle
        self.position_adapters = position_adapters
        self.market_adapters = market_adapters or []
        self.pendle_history_client = pendle_history_client
        self.pt_fixed_yield_overrides = pt_fixed_yield_overrides or {}
        self.progress_callback = progress_callback

    def _report_progress(self, message: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback(message)

    @staticmethod
    def _normalize_address(value: str) -> str:
        value = value.strip()
        if value.startswith("0x"):
            return value.lower()
        return value

    def _write_data_quality(self, issues: list[DataQualityIssue]) -> int:
        if not issues:
            return 0

        rows = [
            {
                "as_of_ts_utc": issue.as_of_ts_utc,
                "stage": issue.stage,
                "protocol_code": issue.protocol_code,
                "chain_code": issue.chain_code,
                "wallet_address": issue.wallet_address,
                "market_ref": issue.market_ref,
                "error_type": issue.error_type,
                "error_message": issue.error_message,
                "payload_json": issue.payload_json,
            }
            for issue in issues
        ]
        self.session.execute(insert(DataQuality).values(rows))
        return len(rows)

    def _build_component_exception_issue(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        error_type: str,
        error_message: str,
        protocol_code: str | None = None,
        payload_json: dict[str, Any] | None = None,
    ) -> DataQualityIssue:
        return DataQualityIssue(
            as_of_ts_utc=as_of_ts_utc,
            stage=stage,
            error_type=error_type,
            error_message=error_message,
            protocol_code=protocol_code,
            payload_json=payload_json,
        )

    def _write_prices(self, *, quotes: list[PriceQuote], as_of_ts_utc: datetime) -> int:
        rows = [
            {
                "ts_utc": as_of_ts_utc,
                "token_id": quote.token_id,
                "price_usd": quote.price_usd,
                "source": quote.source,
                "confidence": None,
            }
            for quote in quotes
        ]

        if not rows:
            return 0

        stmt = insert(Price).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Price.ts_utc, Price.token_id, Price.source],
            set_={
                "price_usd": stmt.excluded.price_usd,
                "confidence": stmt.excluded.confidence,
            },
        )
        self.session.execute(stmt)
        return len(rows)

    def _fetch_price_quotes(
        self,
        *,
        requests: list[PriceRequest],
        as_of_ts_utc: datetime,
        stage: str,
    ) -> tuple[list[PriceQuote], list[DataQualityIssue], int]:
        if self.price_oracle is None:
            return [], [], 0

        try:
            result = self.price_oracle.fetch_prices(requests, as_of_ts_utc=as_of_ts_utc)
        except Exception as exc:
            issue = self._build_component_exception_issue(
                as_of_ts_utc=as_of_ts_utc,
                stage=stage,
                error_type="price_oracle_exception",
                error_message="price oracle raised unexpected exception",
                payload_json={"exception_class": exc.__class__.__name__, "detail": str(exc)},
            )
            return [], [issue], 1
        return result.quotes, result.issues, 0

    def _token_select(self) -> Select[tuple[int, str, str, str]]:
        return select(Token.token_id, Chain.chain_code, Token.address_or_mint, Token.symbol).join(
            Chain, Chain.chain_id == Token.chain_id
        )

    def _price_map_for_all_tokens(
        self, *, as_of_ts_utc: datetime, stage: str
    ) -> tuple[dict[tuple[str, str], Decimal], list[DataQualityIssue], int]:
        """Fetch prices for all seeded tokens and return lookup map + issues."""

        if self.price_oracle is None:
            return {}, [], 0

        token_rows = self.session.execute(self._token_select()).all()
        if not token_rows:
            return {}, [], 0

        token_requests = [
            PriceRequest(
                token_id=token_id,
                chain_code=chain_code,
                address_or_mint=address_or_mint,
                symbol=symbol,
            )
            for token_id, chain_code, address_or_mint, symbol in token_rows
        ]

        quotes, issues, component_failures = self._fetch_price_quotes(
            requests=token_requests,
            as_of_ts_utc=as_of_ts_utc,
            stage=stage,
        )
        self._write_prices(quotes=quotes, as_of_ts_utc=as_of_ts_utc)
        symbol_lookup: dict[tuple[str, str], str] = {}
        for _token_id, chain_code, address_or_mint, symbol in token_rows:
            symbol_key = (chain_code, symbol.strip().upper())
            normalized_address = self._normalize_address(address_or_mint)
            symbol_lookup.setdefault(symbol_key, normalized_address)

        price_map: dict[tuple[str, str], Decimal] = {}
        for quote in quotes:
            normalized_address = self._normalize_address(quote.address_or_mint)
            price_map[(quote.chain_code, normalized_address)] = quote.price_usd

        for (chain_code, symbol), token_address in symbol_lookup.items():
            price = price_map.get((chain_code, token_address))
            if price is not None:
                price_map[(chain_code, f"symbol:{symbol}")] = price

        return price_map, issues, component_failures

    def _resolve_wallet_ids(self) -> dict[str, int]:
        rows = self.session.execute(select(Wallet.address, Wallet.wallet_id)).all()
        return {self._normalize_address(address): wallet_id for address, wallet_id in rows}

    def _resolve_wallet_product_ids(self) -> dict[int, int]:
        rows = self.session.execute(
            select(WalletProductMap.wallet_id, WalletProductMap.product_id)
        ).all()
        return {wallet_id: product_id for wallet_id, product_id in rows}

    def _resolve_market_ids(self) -> dict[tuple[str, str, str], int]:
        rows = self.session.execute(
            select(
                ProtocolModel.protocol_code,
                Chain.chain_code,
                Market.market_address,
                Market.market_id,
            )
            .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
        ).all()
        return {
            (protocol_code, chain_code, self._normalize_address(market_address)): market_id
            for protocol_code, chain_code, market_address, market_id in rows
        }

    def _resolve_market_details(self) -> dict[int, _MarketDetail]:
        base_token = Token.__table__.alias("base_token")
        collateral_token = Token.__table__.alias("collateral_token")
        rows = self.session.execute(
            select(
                Market.market_id,
                Market.protocol_id,
                ProtocolModel.protocol_code,
                Market.chain_id,
                Chain.chain_code,
                Market.market_kind,
                Market.display_name,
                Market.base_asset_token_id,
                base_token.c.address_or_mint,
                base_token.c.symbol,
                Market.collateral_token_id,
                collateral_token.c.address_or_mint,
                collateral_token.c.symbol,
                Market.metadata_json,
            )
            .join(ProtocolModel, ProtocolModel.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .outerjoin(base_token, base_token.c.token_id == Market.base_asset_token_id)
            .outerjoin(collateral_token, collateral_token.c.token_id == Market.collateral_token_id)
        ).all()
        return {
            market_id: _MarketDetail(
                market_id=market_id,
                protocol_id=protocol_id,
                protocol_code=protocol_code,
                chain_id=chain_id,
                chain_code=chain_code,
                market_kind=market_kind or "other",
                display_name=display_name or market_id,
                base_asset_token_id=base_asset_token_id,
                base_asset_address=base_asset_address,
                base_asset_symbol=base_asset_symbol,
                collateral_token_id=collateral_token_id,
                collateral_token_address=collateral_token_address,
                collateral_symbol=collateral_symbol,
                metadata_json=metadata_json if isinstance(metadata_json, dict) else None,
            )
            for (
                market_id,
                protocol_id,
                protocol_code,
                chain_id,
                chain_code,
                market_kind,
                display_name,
                base_asset_token_id,
                base_asset_address,
                base_asset_symbol,
                collateral_token_id,
                collateral_token_address,
                collateral_symbol,
                metadata_json,
            ) in rows
        }

    @staticmethod
    def _is_pt_collateral_symbol(symbol: str | None) -> bool:
        return bool(symbol and symbol.strip().upper().startswith("PT-"))

    def _load_position_fixed_yield_cache(
        self, position_keys: list[str]
    ) -> dict[str, PositionFixedYieldCache]:
        if not position_keys:
            return {}
        rows = self.session.scalars(
            select(PositionFixedYieldCache).where(
                PositionFixedYieldCache.position_key.in_(position_keys)
            )
        ).all()
        return {row.position_key: row for row in rows}

    def _upsert_position_fixed_yield_cache_row(
        self,
        *,
        position: PositionSnapshotInput,
        collateral_symbol: str,
        fixed_apy: Decimal,
        lot_count: int,
        first_acquired_at_utc: datetime | None,
        last_refreshed_at_utc: datetime,
        metadata_json: dict[str, Any],
    ) -> None:
        row = {
            "position_key": position.position_key,
            "protocol_code": position.protocol_code,
            "chain_code": position.chain_code,
            "wallet_address": self._normalize_address(position.wallet_address),
            "market_ref": self._normalize_address(position.market_ref),
            "collateral_symbol": collateral_symbol,
            "fixed_apy": fixed_apy,
            "source": "pendle_history",
            "position_size_native_at_refresh": position.collateral_amount or ZERO,
            "position_size_usd_at_refresh": position.collateral_usd or ZERO,
            "lot_count": lot_count,
            "first_acquired_at_utc": first_acquired_at_utc,
            "last_refreshed_at_utc": last_refreshed_at_utc,
            "metadata_json": metadata_json,
        }
        stmt = insert(PositionFixedYieldCache).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=[PositionFixedYieldCache.position_key],
            set_={
                "protocol_code": stmt.excluded.protocol_code,
                "chain_code": stmt.excluded.chain_code,
                "wallet_address": stmt.excluded.wallet_address,
                "market_ref": stmt.excluded.market_ref,
                "collateral_symbol": stmt.excluded.collateral_symbol,
                "fixed_apy": stmt.excluded.fixed_apy,
                "source": stmt.excluded.source,
                "position_size_native_at_refresh": stmt.excluded.position_size_native_at_refresh,
                "position_size_usd_at_refresh": stmt.excluded.position_size_usd_at_refresh,
                "lot_count": stmt.excluded.lot_count,
                "first_acquired_at_utc": stmt.excluded.first_acquired_at_utc,
                "last_refreshed_at_utc": stmt.excluded.last_refreshed_at_utc,
                "metadata_json": stmt.excluded.metadata_json,
            },
        )
        self.session.execute(stmt)

    def _resolve_pt_fixed_apy(
        self,
        *,
        position: PositionSnapshotInput,
        market_detail: _MarketDetail,
        chain_id: int,
    ) -> tuple[
        Decimal | None, int, datetime | None, dict[str, Any] | None, DataQualityIssue | None
    ]:
        if self.pendle_history_client is None:
            return (
                None,
                0,
                None,
                None,
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="pt_fixed_apy_refresh_failed",
                    error_message="Pendle history client is not configured",
                    protocol_code=position.protocol_code,
                    chain_code=position.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={"position_key": position.position_key},
                ),
            )

        pt_address = market_detail.collateral_token_address
        if not pt_address:
            return (
                None,
                0,
                None,
                None,
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="pt_fixed_apy_unresolved",
                    error_message="PT collateral token address is missing",
                    protocol_code=position.protocol_code,
                    chain_code=position.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={"position_key": position.position_key},
                ),
            )

        try:
            market_addresses = self.pendle_history_client.get_market_addresses_for_pt(
                chain_id=chain_id,
                pt_token_address=pt_address,
            )
            trades = self.pendle_history_client.get_wallet_trades(
                chain_id=chain_id,
                wallet_address=position.wallet_address,
            )
        except Exception as exc:
            return (
                None,
                0,
                None,
                None,
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="pt_fixed_apy_refresh_failed",
                    error_message=str(exc),
                    protocol_code=position.protocol_code,
                    chain_code=position.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={
                        "position_key": position.position_key,
                        "collateral_symbol": market_detail.collateral_symbol,
                        "collateral_token_address": pt_address,
                    },
                ),
            )

        matched_trades = [trade for trade in trades if trade.market_address in market_addresses]
        lots: list[tuple[Decimal, Decimal]] = []
        first_acquired_at_utc: datetime | None = None
        for trade in matched_trades:
            if trade.pt_notional <= ZERO:
                continue
            if trade.action == "BUY_PT":
                lots.append((trade.pt_notional, trade.implied_apy))
                if first_acquired_at_utc is None:
                    first_acquired_at_utc = trade.timestamp
                continue
            if trade.action == "SELL_PT":
                remaining_to_sell = trade.pt_notional
                while remaining_to_sell > ZERO and lots:
                    lot_amount, lot_apy = lots[0]
                    if lot_amount <= remaining_to_sell:
                        remaining_to_sell -= lot_amount
                        lots.pop(0)
                    else:
                        lots[0] = (lot_amount - remaining_to_sell, lot_apy)
                        remaining_to_sell = ZERO

        remaining_pt = sum((amount for amount, _apy in lots), ZERO)
        if remaining_pt <= ZERO:
            return (
                None,
                0,
                first_acquired_at_utc,
                {
                    "matched_market_count": len(market_addresses),
                    "matched_trade_count": len(matched_trades),
                    "remaining_pt": str(remaining_pt),
                },
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="pt_fixed_apy_unresolved",
                    error_message="Pendle history did not leave an open PT lot to value",
                    protocol_code=position.protocol_code,
                    chain_code=position.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={
                        "position_key": position.position_key,
                        "collateral_symbol": market_detail.collateral_symbol,
                        "matched_market_count": len(market_addresses),
                        "matched_trade_count": len(matched_trades),
                    },
                ),
            )

        weighted_fixed_apy = sum((amount * apy for amount, apy in lots), ZERO) / remaining_pt
        metadata_json = {
            "matched_market_addresses": sorted(market_addresses),
            "matched_trade_count": len(matched_trades),
            "remaining_pt": str(remaining_pt),
        }
        return weighted_fixed_apy, len(lots), first_acquired_at_utc, metadata_json, None

    def _apply_morpho_pt_fixed_yields(
        self,
        *,
        positions: list[PositionSnapshotInput],
        market_ids: dict[tuple[str, str, str], int],
        market_details: dict[int, _MarketDetail],
        as_of_ts_utc: datetime,
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        if self.pendle_history_client is None and not self.pt_fixed_yield_overrides:
            return positions, []

        pt_positions: list[tuple[PositionSnapshotInput, _MarketDetail]] = []
        for position in positions:
            if position.protocol_code != "morpho" or (position.collateral_amount or ZERO) <= ZERO:
                continue
            market_id = market_ids.get(
                (
                    position.protocol_code,
                    position.chain_code,
                    self._normalize_address(position.market_ref),
                )
            )
            if market_id is None:
                continue
            detail = market_details.get(market_id)
            if detail is None or not self._is_pt_collateral_symbol(detail.collateral_symbol):
                continue
            pt_positions.append((position, detail))

        if not pt_positions:
            return positions, []

        cache_rows = self._load_position_fixed_yield_cache(
            [position.position_key for position, _detail in pt_positions]
        )
        issues: list[DataQualityIssue] = []
        resolved: dict[str, PositionSnapshotInput] = {}

        for position, detail in pt_positions:
            override = self.pt_fixed_yield_overrides.get(position.position_key)
            if override is not None:
                resolved[position.position_key] = replace(
                    position,
                    supply_apy=override.fixed_apy,
                    reward_apy=ZERO,
                )
                continue

            cache_row = cache_rows.get(position.position_key)
            current_collateral_amount = position.collateral_amount or ZERO
            if (
                cache_row is not None
                and cache_row.position_size_native_at_refresh > ZERO
                and current_collateral_amount
                <= cache_row.position_size_native_at_refresh * PT_BALANCE_GROWTH_THRESHOLD
            ):
                resolved[position.position_key] = replace(
                    position,
                    supply_apy=cache_row.fixed_apy,
                    reward_apy=ZERO,
                )
                continue

            chain_id = PENDLE_CHAIN_IDS.get(position.chain_code)
            if chain_id is None:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="pt_fixed_apy_unresolved",
                        error_message="Pendle chain mapping is missing for PT collateral position",
                        protocol_code=position.protocol_code,
                        chain_code=position.chain_code,
                        wallet_address=position.wallet_address,
                        market_ref=position.market_ref,
                        payload_json={"position_key": position.position_key},
                    )
                )
                resolved[position.position_key] = replace(
                    position, supply_apy=ZERO, reward_apy=ZERO
                )
                continue

            fixed_apy, lot_count, first_acquired_at_utc, metadata_json, issue = (
                self._resolve_pt_fixed_apy(
                    position=position,
                    market_detail=detail,
                    chain_id=chain_id,
                )
            )
            if issue is not None:
                issues.append(issue)
            if fixed_apy is None:
                resolved[position.position_key] = replace(
                    position, supply_apy=ZERO, reward_apy=ZERO
                )
                continue

            self._upsert_position_fixed_yield_cache_row(
                position=position,
                collateral_symbol=detail.collateral_symbol or "PT",
                fixed_apy=fixed_apy,
                lot_count=lot_count,
                first_acquired_at_utc=first_acquired_at_utc,
                last_refreshed_at_utc=as_of_ts_utc,
                metadata_json=metadata_json or {},
            )
            resolved[position.position_key] = replace(
                position, supply_apy=fixed_apy, reward_apy=ZERO
            )

        updated_positions = [
            resolved.get(position.position_key, position) for position in positions
        ]
        return updated_positions, issues

    def _resolve_market_exposure_ids_by_market(self) -> dict[int, int]:
        rows = self.session.execute(
            select(
                MarketExposureComponent.market_id,
                MarketExposureComponent.market_exposure_id,
            )
        ).all()
        return {market_id: market_exposure_id for market_id, market_exposure_id in rows}

    def _upsert_positions(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        stmt = insert(Position).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Position.position_key],
            set_={
                "wallet_id": stmt.excluded.wallet_id,
                "product_id": stmt.excluded.product_id,
                "protocol_id": stmt.excluded.protocol_id,
                "chain_id": stmt.excluded.chain_id,
                "market_id": stmt.excluded.market_id,
                "market_exposure_id": stmt.excluded.market_exposure_id,
                "exposure_class": stmt.excluded.exposure_class,
                "status": "open",
                "display_name": stmt.excluded.display_name,
                "last_seen_at_utc": stmt.excluded.last_seen_at_utc,
            },
        )
        self.session.execute(stmt)

    def _resolve_position_ids(self, position_keys: list[str]) -> dict[str, int]:
        if not position_keys:
            return {}
        rows = self.session.execute(
            select(Position.position_key, Position.position_id).where(
                Position.position_key.in_(position_keys)
            )
        ).all()
        return {position_key: position_id for position_key, position_id in rows}

    def _write_snapshot_legs(
        self,
        *,
        leg_rows_by_key: dict[str, list[dict[str, object]]],
        as_of_ts_utc: datetime,
    ) -> None:
        if not leg_rows_by_key:
            return
        snapshot_rows = self.session.execute(
            select(PositionSnapshot.snapshot_id, PositionSnapshot.position_key).where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                PositionSnapshot.position_key.in_(list(leg_rows_by_key)),
            )
        ).all()
        snapshot_ids = {position_key: snapshot_id for snapshot_id, position_key in snapshot_rows}
        target_snapshot_ids = [snapshot_id for snapshot_id in snapshot_ids.values()]
        if target_snapshot_ids:
            self.session.execute(
                delete(PositionSnapshotLeg).where(
                    PositionSnapshotLeg.snapshot_id.in_(target_snapshot_ids)
                )
            )

        leg_rows: list[dict[str, object]] = []
        for position_key, rows in leg_rows_by_key.items():
            snapshot_id = snapshot_ids.get(position_key)
            if snapshot_id is None:
                continue
            for row in rows:
                leg_rows.append({**row, "snapshot_id": snapshot_id})

        if not leg_rows:
            return
        stmt = insert(PositionSnapshotLeg).values(leg_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[PositionSnapshotLeg.snapshot_id, PositionSnapshotLeg.leg_type],
            set_={
                "token_id": stmt.excluded.token_id,
                "market_id": stmt.excluded.market_id,
                "amount_native": stmt.excluded.amount_native,
                "usd_value": stmt.excluded.usd_value,
                "rate": stmt.excluded.rate,
                "estimated_daily_cashflow_usd": stmt.excluded.estimated_daily_cashflow_usd,
                "is_collateral": stmt.excluded.is_collateral,
            },
        )
        self.session.execute(stmt)

    def _build_supply_leg(
        self,
        *,
        position: PositionSnapshotInput,
        detail: _MarketDetail,
        token_id: int,
    ) -> dict[str, object]:
        supply_amount = economic_supply_amount(
            supplied_amount=position.supplied_amount,
            collateral_amount=position.collateral_amount,
            collateral_token_id=detail.collateral_token_id,
            collateral_usd=position.collateral_usd,
        )
        supply_usd = economic_supply_usd(
            supplied_usd=position.supplied_usd,
            collateral_usd=position.collateral_usd,
            collateral_token_id=detail.collateral_token_id,
            collateral_amount=position.collateral_amount,
        )
        return {
            "leg_type": "supply",
            "token_id": token_id,
            "market_id": detail.market_id,
            "amount_native": supply_amount,
            "usd_value": supply_usd,
            "rate": position.supply_apy + position.reward_apy,
            "estimated_daily_cashflow_usd": supply_usd
            * (position.supply_apy + position.reward_apy)
            / DAYS_PER_YEAR,
            "is_collateral": token_id == detail.collateral_token_id,
        }

    def _build_borrow_leg(
        self,
        *,
        position: PositionSnapshotInput,
        detail: _MarketDetail,
        token_id: int,
    ) -> dict[str, object]:
        return {
            "leg_type": "borrow",
            "token_id": token_id,
            "market_id": detail.market_id,
            "amount_native": position.borrowed_amount,
            "usd_value": position.borrowed_usd,
            "rate": position.borrow_apy,
            "estimated_daily_cashflow_usd": -(
                position.borrowed_usd * position.borrow_apy / DAYS_PER_YEAR
            ),
            "is_collateral": False,
        }

    def _normalize_position_contract(
        self,
        *,
        position: PositionSnapshotInput,
        wallet_id: int,
        product_id: int | None,
        market_detail: _MarketDetail,
        market_exposure_id: int | None,
    ) -> tuple[dict[str, object] | None, list[dict[str, object]] | None, DataQualityIssue | None]:
        exposure_class = position_exposure_class(
            market_detail.metadata_json, market_detail.protocol_code
        )

        supply_token_id = economic_supply_token_id(
            base_asset_token_id=market_detail.base_asset_token_id,
            collateral_token_id=market_detail.collateral_token_id,
            collateral_amount=position.collateral_amount,
            collateral_usd=position.collateral_usd,
        )
        if supply_token_id is None:
            if exposure_class != "core_lending":
                return None, None, None
            return (
                None,
                None,
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="position_leg_token_missing",
                    error_message="unable to determine supply token for position",
                    protocol_code=market_detail.protocol_code,
                    chain_code=market_detail.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={"position_key": position.position_key},
                ),
            )

        borrow_token_id = market_detail.base_asset_token_id
        if position.borrowed_amount > ZERO and borrow_token_id is None:
            if exposure_class != "core_lending":
                return None, None, None
            return (
                None,
                None,
                DataQualityIssue(
                    as_of_ts_utc=position.as_of_ts_utc,
                    stage="sync_snapshot",
                    error_type="position_leg_token_missing",
                    error_message="unable to determine borrow token for position",
                    protocol_code=market_detail.protocol_code,
                    chain_code=market_detail.chain_code,
                    wallet_address=position.wallet_address,
                    market_ref=position.market_ref,
                    payload_json={"position_key": position.position_key},
                ),
            )

        position_row = {
            "position_key": position.position_key,
            "wallet_id": wallet_id,
            "product_id": product_id,
            "protocol_id": market_detail.protocol_id,
            "chain_id": market_detail.chain_id,
            "market_id": market_detail.market_id,
            "market_exposure_id": market_exposure_id,
            "exposure_class": exposure_class,
            "status": "open",
            "display_name": market_detail.display_name,
            "opened_at_utc": position.as_of_ts_utc,
            "last_seen_at_utc": position.as_of_ts_utc,
        }
        legs = [
            self._build_supply_leg(
                position=position, detail=market_detail, token_id=supply_token_id
            )
        ]
        if position.borrowed_amount > ZERO and borrow_token_id is not None:
            legs.append(
                self._build_borrow_leg(
                    position=position, detail=market_detail, token_id=borrow_token_id
                )
            )
        return position_row, legs, None

    def sync_prices(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        """Fetch token prices and persist into `prices`."""

        if self.price_oracle is None:
            issues = [
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_prices",
                    error_type="price_oracle_missing",
                    error_message="Price oracle is not configured",
                )
            ]
            issue_count = self._write_data_quality(issues)
            return RunnerSummary(rows_written=0, issues_written=issue_count, component_failures=0)

        token_rows = self.session.execute(self._token_select()).all()
        requests = [
            PriceRequest(
                token_id=token_id,
                chain_code=chain_code,
                address_or_mint=address_or_mint,
                symbol=symbol,
            )
            for token_id, chain_code, address_or_mint, symbol in token_rows
        ]

        quotes, issues, component_failures = self._fetch_price_quotes(
            requests=requests,
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_prices",
        )
        rows_written = self._write_prices(quotes=quotes, as_of_ts_utc=as_of_ts_utc)
        issue_count = self._write_data_quality(issues)
        return RunnerSummary(
            rows_written=rows_written,
            issues_written=issue_count,
            component_failures=component_failures,
        )

    def sync_snapshot(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        """Collect adapter positions and persist position snapshots."""

        price_start = time.perf_counter()
        self._report_progress("sync snapshot stage=price_map status=start")
        prices_by_token, price_issues, component_failures = self._price_map_for_all_tokens(
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_snapshot",
        )
        self._report_progress(
            "sync snapshot stage=price_map status=complete "
            f"duration_s={time.perf_counter() - price_start:.2f} "
            f"prices={len(prices_by_token)} issues={len(price_issues)} "
            f"component_failures={component_failures}"
        )

        all_positions: list[PositionSnapshotInput] = []
        all_issues: list[DataQualityIssue] = list(price_issues)

        collect_start = time.perf_counter()
        self._report_progress(
            "sync snapshot stage=collect_positions status=start "
            f"adapter_count={len(self.position_adapters)}"
        )
        if self.position_adapters:
            max_workers = min(4, len(self.position_adapters))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {}
                for adapter in self.position_adapters:
                    self._report_progress(
                        f"sync snapshot adapter={adapter.protocol_code} status=start"
                    )
                    future = executor.submit(
                        adapter.collect_positions,
                        as_of_ts_utc=as_of_ts_utc,
                        prices_by_token=prices_by_token,
                    )
                    future_map[future] = (adapter, time.perf_counter())

                for future in as_completed(future_map):
                    adapter, started_at = future_map[future]
                    duration_s = time.perf_counter() - started_at
                    try:
                        positions, issues = future.result()
                    except Exception as exc:
                        component_failures += 1
                        all_issues.append(
                            self._build_component_exception_issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="position_adapter_exception",
                                error_message="position adapter raised unexpected exception",
                                protocol_code=adapter.protocol_code,
                                payload_json={
                                    "exception_class": exc.__class__.__name__,
                                    "detail": str(exc),
                                },
                            )
                        )
                        self._report_progress(
                            f"sync snapshot adapter={adapter.protocol_code} status=error "
                            f"duration_s={duration_s:.2f} "
                            f"exception_class={exc.__class__.__name__}"
                        )
                        continue
                    all_positions.extend(positions)
                    all_issues.extend(issues)
                    self._report_progress(
                        f"sync snapshot adapter={adapter.protocol_code} status=complete "
                        f"duration_s={duration_s:.2f} positions={len(positions)} "
                        f"issues={len(issues)}"
                    )
        self._report_progress(
            "sync snapshot stage=collect_positions status=complete "
            f"duration_s={time.perf_counter() - collect_start:.2f} "
            f"positions={len(all_positions)} issues={len(all_issues)}"
        )

        market_ids = self._resolve_market_ids()
        market_details = self._resolve_market_details()
        all_positions, pt_fixed_yield_issues = self._apply_morpho_pt_fixed_yields(
            positions=all_positions,
            market_ids=market_ids,
            market_details=market_details,
            as_of_ts_utc=as_of_ts_utc,
        )
        all_issues.extend(pt_fixed_yield_issues)

        ensure_market_exposures(self.session)
        wallet_ids = self._resolve_wallet_ids()
        wallet_product_ids = self._resolve_wallet_product_ids()
        market_exposure_ids = self._resolve_market_exposure_ids_by_market()

        position_definition_rows: dict[str, dict[str, object]] = {}
        leg_rows_by_key: dict[str, list[dict[str, object]]] = {}
        snapshot_rows: list[dict[str, object]] = []

        for position in all_positions:
            wallet_id = wallet_ids.get(self._normalize_address(position.wallet_address))
            market_id = market_ids.get(
                (
                    position.protocol_code,
                    position.chain_code,
                    self._normalize_address(position.market_ref),
                )
            )
            if wallet_id is None or market_id is None:
                all_issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="dimension_missing",
                        error_message="wallet or market dimension missing for position",
                        protocol_code=position.protocol_code,
                        chain_code=position.chain_code,
                        wallet_address=position.wallet_address,
                        market_ref=position.market_ref,
                    )
                )
                continue

            snapshot_rows.append(
                {
                    "as_of_ts_utc": position.as_of_ts_utc,
                    "block_number_or_slot": position.block_number_or_slot,
                    "wallet_id": wallet_id,
                    "market_id": market_id,
                    "position_key": position.position_key,
                    "supplied_amount": position.supplied_amount,
                    "supplied_usd": position.supplied_usd,
                    "collateral_amount": position.collateral_amount,
                    "collateral_usd": position.collateral_usd,
                    "borrowed_amount": position.borrowed_amount,
                    "borrowed_usd": position.borrowed_usd,
                    "supply_apy": position.supply_apy,
                    "borrow_apy": position.borrow_apy,
                    "reward_apy": position.reward_apy,
                    "equity_usd": position.equity_usd,
                    "health_factor": position.health_factor,
                    "ltv": position.ltv,
                    "source": position.source,
                    "position_id": None,
                }
            )

            market_detail = market_details.get(market_id)
            if market_detail is None:
                continue
            position_row, legs, normalization_issue = self._normalize_position_contract(
                position=position,
                wallet_id=wallet_id,
                product_id=wallet_product_ids.get(wallet_id),
                market_detail=market_detail,
                market_exposure_id=market_exposure_ids.get(market_id),
            )
            if normalization_issue is not None:
                all_issues.append(normalization_issue)
                continue
            if position_row is None or legs is None:
                continue
            position_definition_rows[position.position_key] = position_row
            leg_rows_by_key[position.position_key] = legs

        self._upsert_positions(list(position_definition_rows.values()))
        position_ids = self._resolve_position_ids(list(position_definition_rows))
        for row in snapshot_rows:
            position_id = position_ids.get(str(row["position_key"]))
            if position_id is not None:
                row["position_id"] = position_id

        if snapshot_rows:
            stmt = insert(PositionSnapshot).values(snapshot_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[PositionSnapshot.as_of_ts_utc, PositionSnapshot.position_key],
                set_={
                    "position_id": stmt.excluded.position_id,
                    "supplied_amount": stmt.excluded.supplied_amount,
                    "supplied_usd": stmt.excluded.supplied_usd,
                    "collateral_amount": stmt.excluded.collateral_amount,
                    "collateral_usd": stmt.excluded.collateral_usd,
                    "borrowed_amount": stmt.excluded.borrowed_amount,
                    "borrowed_usd": stmt.excluded.borrowed_usd,
                    "supply_apy": stmt.excluded.supply_apy,
                    "borrow_apy": stmt.excluded.borrow_apy,
                    "reward_apy": stmt.excluded.reward_apy,
                    "equity_usd": stmt.excluded.equity_usd,
                    "health_factor": stmt.excluded.health_factor,
                    "ltv": stmt.excluded.ltv,
                    "source": stmt.excluded.source,
                    "wallet_id": stmt.excluded.wallet_id,
                    "market_id": stmt.excluded.market_id,
                    "block_number_or_slot": stmt.excluded.block_number_or_slot,
                },
            )
            self.session.execute(stmt)
            self._write_snapshot_legs(leg_rows_by_key=leg_rows_by_key, as_of_ts_utc=as_of_ts_utc)

        issue_count = self._write_data_quality(all_issues)
        return RunnerSummary(
            rows_written=len(snapshot_rows),
            issues_written=issue_count,
            component_failures=component_failures,
        )

    def sync_markets(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        """Collect adapter market data and persist market snapshots."""

        price_start = time.perf_counter()
        self._report_progress("sync markets stage=price_map status=start")
        prices_by_token, price_issues, component_failures = self._price_map_for_all_tokens(
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_markets",
        )
        self._report_progress(
            "sync markets stage=price_map status=complete "
            f"duration_s={time.perf_counter() - price_start:.2f} "
            f"prices={len(prices_by_token)} issues={len(price_issues)} "
            f"component_failures={component_failures}"
        )

        all_markets: list[MarketSnapshotInput] = []
        all_issues: list[DataQualityIssue] = list(price_issues)

        collect_start = time.perf_counter()
        self._report_progress(
            "sync markets stage=collect_markets status=start "
            f"adapter_count={len(self.market_adapters)}"
        )
        if self.market_adapters:
            max_workers = min(4, len(self.market_adapters))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {}
                for adapter in self.market_adapters:
                    self._report_progress(
                        f"sync markets adapter={adapter.protocol_code} status=start"
                    )
                    future = executor.submit(
                        adapter.collect_markets,
                        as_of_ts_utc=as_of_ts_utc,
                        prices_by_token=prices_by_token,
                    )
                    future_map[future] = (adapter, time.perf_counter())

                for future in as_completed(future_map):
                    adapter, started_at = future_map[future]
                    duration_s = time.perf_counter() - started_at
                    try:
                        markets, issues = future.result()
                    except Exception as exc:
                        component_failures += 1
                        all_issues.append(
                            self._build_component_exception_issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_markets",
                                error_type="market_adapter_exception",
                                error_message="market adapter raised unexpected exception",
                                protocol_code=adapter.protocol_code,
                                payload_json={
                                    "exception_class": exc.__class__.__name__,
                                    "detail": str(exc),
                                },
                            )
                        )
                        self._report_progress(
                            f"sync markets adapter={adapter.protocol_code} status=error "
                            f"duration_s={duration_s:.2f} "
                            f"exception_class={exc.__class__.__name__}"
                        )
                        continue
                    all_markets.extend(markets)
                    all_issues.extend(issues)
                    self._report_progress(
                        f"sync markets adapter={adapter.protocol_code} status=complete "
                        f"duration_s={duration_s:.2f} markets={len(markets)} issues={len(issues)}"
                    )
        self._report_progress(
            "sync markets stage=collect_markets status=complete "
            f"duration_s={time.perf_counter() - collect_start:.2f} "
            f"markets={len(all_markets)} issues={len(all_issues)}"
        )

        market_ids = self._resolve_market_ids()
        rows: list[dict[str, object]] = []
        for market in all_markets:
            market_id = market_ids.get(
                (
                    market.protocol_code,
                    market.chain_code,
                    self._normalize_address(market.market_ref),
                )
            )
            if market_id is None:
                all_issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_markets",
                        error_type="dimension_missing",
                        error_message="market dimension missing for market snapshot",
                        protocol_code=market.protocol_code,
                        chain_code=market.chain_code,
                        market_ref=market.market_ref,
                    )
                )
                continue

            rows.append(
                {
                    "as_of_ts_utc": market.as_of_ts_utc,
                    "block_number_or_slot": market.block_number_or_slot,
                    "market_id": market_id,
                    "total_supply_usd": market.total_supply_usd,
                    "total_borrow_usd": market.total_borrow_usd,
                    "utilization": market.utilization,
                    "supply_apy": market.supply_apy,
                    "borrow_apy": market.borrow_apy,
                    "available_liquidity_usd": market.available_liquidity_usd,
                    "max_ltv": market.max_ltv,
                    "liquidation_threshold": market.liquidation_threshold,
                    "liquidation_penalty": market.liquidation_penalty,
                    "caps_json": market.caps_json,
                    "irm_params_json": market.irm_params_json,
                    "source": market.source,
                }
            )

        if rows:
            stmt = insert(MarketSnapshot).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    MarketSnapshot.as_of_ts_utc,
                    MarketSnapshot.market_id,
                    MarketSnapshot.source,
                ],
                set_={
                    "block_number_or_slot": stmt.excluded.block_number_or_slot,
                    "total_supply_usd": stmt.excluded.total_supply_usd,
                    "total_borrow_usd": stmt.excluded.total_borrow_usd,
                    "utilization": stmt.excluded.utilization,
                    "supply_apy": stmt.excluded.supply_apy,
                    "borrow_apy": stmt.excluded.borrow_apy,
                    "available_liquidity_usd": stmt.excluded.available_liquidity_usd,
                    "max_ltv": stmt.excluded.max_ltv,
                    "liquidation_threshold": stmt.excluded.liquidation_threshold,
                    "liquidation_penalty": stmt.excluded.liquidation_penalty,
                    "caps_json": stmt.excluded.caps_json,
                    "irm_params_json": stmt.excluded.irm_params_json,
                    "market_id": stmt.excluded.market_id,
                },
            )
            self.session.execute(stmt)

        issue_count = self._write_data_quality(all_issues)
        return RunnerSummary(
            rows_written=len(rows),
            issues_written=issue_count,
            component_failures=component_failures,
        )
