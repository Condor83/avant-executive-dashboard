"""Snapshot ingestion runner and persistence helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import MarketsConfig
from core.db.models import (
    Chain,
    DataQuality,
    Market,
    PositionSnapshot,
    Price,
    Token,
    Wallet,
)
from core.db.models import (
    Protocol as ProtocolModel,
)
from core.pricing import PriceOracle
from core.types import DataQualityIssue, PositionSnapshotInput, PriceRequest


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


@dataclass(frozen=True)
class RunnerSummary:
    """Generic runner summary used by CLI output."""

    rows_written: int
    issues_written: int


class SnapshotRunner:
    """Coordinates pricing, adapters, and DB persistence for sync commands."""

    def __init__(
        self,
        *,
        session: Session,
        markets_config: MarketsConfig,
        price_oracle: PriceOracle | None,
        position_adapters: list[PositionAdapter],
    ) -> None:
        self.session = session
        self.markets_config = markets_config
        self.price_oracle = price_oracle
        self.position_adapters = position_adapters

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

    def _token_select(self) -> Select[tuple[int, str, str, str]]:
        return select(Token.token_id, Chain.chain_code, Token.address_or_mint, Token.symbol).join(
            Chain, Chain.chain_id == Token.chain_id
        )

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
            return RunnerSummary(rows_written=0, issues_written=issue_count)

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

        result = self.price_oracle.fetch_prices(requests, as_of_ts_utc=as_of_ts_utc)

        rows = [
            {
                "ts_utc": as_of_ts_utc,
                "token_id": quote.token_id,
                "price_usd": quote.price_usd,
                "source": quote.source,
                "confidence": None,
            }
            for quote in result.quotes
        ]

        if rows:
            stmt = insert(Price).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Price.ts_utc, Price.token_id, Price.source],
                set_={
                    "price_usd": stmt.excluded.price_usd,
                    "confidence": stmt.excluded.confidence,
                },
            )
            self.session.execute(stmt)

        issue_count = self._write_data_quality(result.issues)
        return RunnerSummary(rows_written=len(rows), issues_written=issue_count)

    def _price_map_for_wallet_balances(
        self, *, as_of_ts_utc: datetime
    ) -> tuple[dict[tuple[str, str], Decimal], int]:
        """Ensure wallet-balance tokens are priced for snapshot runs."""

        if self.price_oracle is None:
            return {}, 0

        balance_tokens: set[tuple[str, str]] = set()
        for chain_code, chain_config in self.markets_config.wallet_balances.items():
            for token in chain_config.tokens:
                balance_tokens.add((chain_code, self._normalize_address(token.address)))

        if not balance_tokens:
            return {}, 0

        token_rows = self.session.execute(self._token_select()).all()
        token_requests: list[PriceRequest] = []
        for token_id, chain_code, address_or_mint, symbol in token_rows:
            token_key = (chain_code, self._normalize_address(address_or_mint))
            if token_key in balance_tokens:
                token_requests.append(
                    PriceRequest(
                        token_id=token_id,
                        chain_code=chain_code,
                        address_or_mint=address_or_mint,
                        symbol=symbol,
                    )
                )

        result = self.price_oracle.fetch_prices(token_requests, as_of_ts_utc=as_of_ts_utc)
        price_rows = [
            {
                "ts_utc": as_of_ts_utc,
                "token_id": quote.token_id,
                "price_usd": quote.price_usd,
                "source": quote.source,
                "confidence": None,
            }
            for quote in result.quotes
        ]
        if price_rows:
            stmt = insert(Price).values(price_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Price.ts_utc, Price.token_id, Price.source],
                set_={
                    "price_usd": stmt.excluded.price_usd,
                    "confidence": stmt.excluded.confidence,
                },
            )
            self.session.execute(stmt)

        issue_count = self._write_data_quality(result.issues)
        price_map = {
            (quote.chain_code, self._normalize_address(quote.address_or_mint)): quote.price_usd
            for quote in result.quotes
        }
        return price_map, issue_count

    def _resolve_wallet_ids(self) -> dict[str, int]:
        rows = self.session.execute(select(Wallet.address, Wallet.wallet_id)).all()
        return {self._normalize_address(address): wallet_id for address, wallet_id in rows}

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

    def sync_snapshot(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        """Collect adapter positions and persist position snapshots."""

        prices_by_token, price_issue_count = self._price_map_for_wallet_balances(
            as_of_ts_utc=as_of_ts_utc
        )

        all_positions: list[PositionSnapshotInput] = []
        all_issues: list[DataQualityIssue] = []

        for adapter in self.position_adapters:
            positions, issues = adapter.collect_positions(
                as_of_ts_utc=as_of_ts_utc,
                prices_by_token=prices_by_token,
            )
            all_positions.extend(positions)
            all_issues.extend(issues)

        wallet_ids = self._resolve_wallet_ids()
        market_ids = self._resolve_market_ids()

        rows: list[dict[str, object]] = []
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

            rows.append(
                {
                    "as_of_ts_utc": position.as_of_ts_utc,
                    "block_number_or_slot": position.block_number_or_slot,
                    "wallet_id": wallet_id,
                    "market_id": market_id,
                    "position_key": position.position_key,
                    "supplied_amount": position.supplied_amount,
                    "supplied_usd": position.supplied_usd,
                    "borrowed_amount": position.borrowed_amount,
                    "borrowed_usd": position.borrowed_usd,
                    "supply_apy": position.supply_apy,
                    "borrow_apy": position.borrow_apy,
                    "reward_apy": position.reward_apy,
                    "equity_usd": position.equity_usd,
                    "health_factor": position.health_factor,
                    "ltv": position.ltv,
                    "source": position.source,
                }
            )

        if rows:
            stmt = insert(PositionSnapshot).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[PositionSnapshot.as_of_ts_utc, PositionSnapshot.position_key],
                set_={
                    "supplied_amount": stmt.excluded.supplied_amount,
                    "supplied_usd": stmt.excluded.supplied_usd,
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

        issue_count = self._write_data_quality(all_issues)
        return RunnerSummary(rows_written=len(rows), issues_written=issue_count + price_issue_count)

    def sync_markets(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        """Sync market health snapshots (adapter implementations start in Sprint 03)."""

        issue_count = self._write_data_quality(
            [
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_markets",
                    error_type="not_implemented",
                    error_message="market snapshot adapters are introduced in Sprint 03",
                )
            ]
        )
        return RunnerSummary(rows_written=0, issues_written=issue_count)
