"""Daily market overview computation from canonical snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import MarketOverviewDaily, MarketSnapshot, PositionSnapshot

ZERO = Decimal("0")
SOURCE_PRIORITY = {"rpc": 0, "defillama": 1, "debank": 2}


@dataclass(frozen=True)
class _MarketSnapshotRow:
    market_id: int
    source: str
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    utilization: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    available_liquidity_usd: Decimal | None
    max_ltv: Decimal | None
    liquidation_threshold: Decimal | None
    liquidation_penalty: Decimal | None


@dataclass(frozen=True)
class MarketOverviewRow:
    """Derived daily market overview row."""

    business_date: date
    as_of_ts_utc: datetime
    market_id: int
    source: str
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    utilization: Decimal
    available_liquidity_usd: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    spread_apy: Decimal
    avant_supplied_usd: Decimal
    avant_borrowed_usd: Decimal
    avant_supply_share: Decimal | None
    avant_borrow_share: Decimal | None
    max_ltv: Decimal | None
    liquidation_threshold: Decimal | None
    liquidation_penalty: Decimal | None

    def as_insert_dict(self) -> dict[str, object]:
        return {
            "business_date": self.business_date,
            "as_of_ts_utc": self.as_of_ts_utc,
            "market_id": self.market_id,
            "source": self.source,
            "total_supply_usd": self.total_supply_usd,
            "total_borrow_usd": self.total_borrow_usd,
            "utilization": self.utilization,
            "available_liquidity_usd": self.available_liquidity_usd,
            "supply_apy": self.supply_apy,
            "borrow_apy": self.borrow_apy,
            "spread_apy": self.spread_apy,
            "avant_supplied_usd": self.avant_supplied_usd,
            "avant_borrowed_usd": self.avant_borrowed_usd,
            "avant_supply_share": self.avant_supply_share,
            "avant_borrow_share": self.avant_borrow_share,
            "max_ltv": self.max_ltv,
            "liquidation_threshold": self.liquidation_threshold,
            "liquidation_penalty": self.liquidation_penalty,
        }


@dataclass(frozen=True)
class MarketComputationSummary:
    """Daily market overview computation summary for CLI output."""

    business_date: date
    as_of_ts_utc: datetime
    rows_written: int


class MarketEngine:
    """Build and persist daily market overview rows from canonical snapshots."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def compute_daily(self, *, business_date: date) -> MarketComputationSummary:
        start_utc, end_utc = denver_business_bounds_utc(business_date)
        as_of_ts_utc = self._resolve_common_snapshot_ts(start_utc=start_utc, end_utc=end_utc)
        if as_of_ts_utc is None:
            raise ValueError(
                "no common market/position snapshots found for business_date="
                f"{business_date.isoformat()}"
            )

        rows = self._build_rows(business_date=business_date, as_of_ts_utc=as_of_ts_utc)

        self.session.execute(
            delete(MarketOverviewDaily).where(MarketOverviewDaily.business_date == business_date)
        )
        if rows:
            self.session.execute(
                insert(MarketOverviewDaily).values([row.as_insert_dict() for row in rows])
            )

        return MarketComputationSummary(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
            rows_written=len(rows),
        )

    def _resolve_common_snapshot_ts(
        self,
        *,
        start_utc: datetime,
        end_utc: datetime,
    ) -> datetime | None:
        market_ts_subq = (
            select(MarketSnapshot.as_of_ts_utc.label("ts"))
            .where(
                MarketSnapshot.as_of_ts_utc >= start_utc,
                MarketSnapshot.as_of_ts_utc <= end_utc,
            )
            .group_by(MarketSnapshot.as_of_ts_utc)
            .subquery()
        )
        position_ts_subq = (
            select(PositionSnapshot.as_of_ts_utc.label("ts"))
            .where(
                PositionSnapshot.as_of_ts_utc >= start_utc,
                PositionSnapshot.as_of_ts_utc <= end_utc,
            )
            .group_by(PositionSnapshot.as_of_ts_utc)
            .subquery()
        )

        return self.session.scalar(
            select(func.max(market_ts_subq.c.ts)).select_from(
                market_ts_subq.join(position_ts_subq, position_ts_subq.c.ts == market_ts_subq.c.ts)
            )
        )

    def _load_market_snapshots(self, *, as_of_ts_utc: datetime) -> list[_MarketSnapshotRow]:
        raw_rows = self.session.execute(
            select(
                MarketSnapshot.market_id,
                MarketSnapshot.source,
                MarketSnapshot.total_supply_usd,
                MarketSnapshot.total_borrow_usd,
                MarketSnapshot.utilization,
                MarketSnapshot.supply_apy,
                MarketSnapshot.borrow_apy,
                MarketSnapshot.available_liquidity_usd,
                MarketSnapshot.max_ltv,
                MarketSnapshot.liquidation_threshold,
                MarketSnapshot.liquidation_penalty,
            ).where(MarketSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()

        deduped_by_market: dict[int, tuple[int, _MarketSnapshotRow]] = {}
        for row in raw_rows:
            source_priority = SOURCE_PRIORITY.get(row[1], 99)
            snapshot_row = _MarketSnapshotRow(
                market_id=row[0],
                source=row[1],
                total_supply_usd=row[2],
                total_borrow_usd=row[3],
                utilization=row[4],
                supply_apy=row[5],
                borrow_apy=row[6],
                available_liquidity_usd=row[7],
                max_ltv=row[8],
                liquidation_threshold=row[9],
                liquidation_penalty=row[10],
            )
            existing = deduped_by_market.get(snapshot_row.market_id)
            if existing is None or source_priority < existing[0]:
                deduped_by_market[snapshot_row.market_id] = (source_priority, snapshot_row)

        return [deduped_by_market[market_id][1] for market_id in sorted(deduped_by_market)]

    def _load_position_totals_by_market(
        self,
        *,
        as_of_ts_utc: datetime,
    ) -> dict[int, tuple[Decimal, Decimal]]:
        rows = self.session.execute(
            select(
                PositionSnapshot.market_id,
                func.coalesce(func.sum(PositionSnapshot.supplied_usd), ZERO),
                func.coalesce(func.sum(PositionSnapshot.borrowed_usd), ZERO),
            )
            .where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
            .group_by(PositionSnapshot.market_id)
        ).all()

        return {
            market_id: (supplied_usd, borrowed_usd)
            for market_id, supplied_usd, borrowed_usd in rows
        }

    def _build_rows(
        self,
        *,
        business_date: date,
        as_of_ts_utc: datetime,
    ) -> list[MarketOverviewRow]:
        market_rows = self._load_market_snapshots(as_of_ts_utc=as_of_ts_utc)
        position_totals = self._load_position_totals_by_market(as_of_ts_utc=as_of_ts_utc)

        output: list[MarketOverviewRow] = []
        for market_row in market_rows:
            avant_supplied_usd, avant_borrowed_usd = position_totals.get(
                market_row.market_id, (ZERO, ZERO)
            )
            available_liquidity_usd = (
                market_row.available_liquidity_usd
                if market_row.available_liquidity_usd is not None
                else max(market_row.total_supply_usd - market_row.total_borrow_usd, ZERO)
            )
            spread_apy = market_row.supply_apy - market_row.borrow_apy
            avant_supply_share = (
                avant_supplied_usd / market_row.total_supply_usd
                if market_row.total_supply_usd > ZERO
                else None
            )
            avant_borrow_share = (
                avant_borrowed_usd / market_row.total_borrow_usd
                if market_row.total_borrow_usd > ZERO
                else None
            )

            output.append(
                MarketOverviewRow(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    market_id=market_row.market_id,
                    source=market_row.source,
                    total_supply_usd=market_row.total_supply_usd,
                    total_borrow_usd=market_row.total_borrow_usd,
                    utilization=market_row.utilization,
                    available_liquidity_usd=available_liquidity_usd,
                    supply_apy=market_row.supply_apy,
                    borrow_apy=market_row.borrow_apy,
                    spread_apy=spread_apy,
                    avant_supplied_usd=avant_supplied_usd,
                    avant_borrowed_usd=avant_borrowed_usd,
                    avant_supply_share=avant_supply_share,
                    avant_borrow_share=avant_borrow_share,
                    max_ltv=market_row.max_ltv,
                    liquidation_threshold=market_row.liquidation_threshold,
                    liquidation_penalty=market_row.liquidation_penalty,
                )
            )

        return output
