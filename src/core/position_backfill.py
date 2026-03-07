"""Backfill canonical positions and legs from legacy flat position snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.market_exposures import ensure_market_exposures
from core.dashboard_contracts import position_exposure_class
from core.db.models import (
    Market,
    MarketExposureComponent,
    Position,
    PositionSnapshot,
    PositionSnapshotLeg,
    Protocol,
    Wallet,
    WalletProductMap,
)
from core.position_contracts import (
    economic_supply_amount,
    economic_supply_token_id,
    economic_supply_usd,
)

DAYS_PER_YEAR = Decimal("365")
ZERO = Decimal("0")


@dataclass(frozen=True)
class PositionBackfillSummary:
    positions_written: int
    snapshots_linked: int
    legs_written: int


@dataclass(frozen=True)
class _SnapshotDescriptor:
    snapshot_id: int
    as_of_ts_utc: datetime
    position_key: str
    wallet_id: int
    wallet_type: str
    product_id: int | None
    protocol_id: int
    protocol_code: str
    chain_id: int
    market_id: int
    market_kind: str
    market_display_name: str
    metadata_json: dict[str, Any] | None
    base_asset_token_id: int | None
    collateral_token_id: int | None
    supplied_amount: Decimal
    supplied_usd: Decimal
    collateral_amount: Decimal | None
    collateral_usd: Decimal | None
    borrowed_amount: Decimal
    borrowed_usd: Decimal
    supply_apy: Decimal
    reward_apy: Decimal
    borrow_apy: Decimal


def _load_snapshot_descriptors(
    session: Session,
    *,
    as_of_ts_utc: datetime | None,
) -> list[_SnapshotDescriptor]:
    stmt = (
        select(
            PositionSnapshot.snapshot_id,
            PositionSnapshot.as_of_ts_utc,
            PositionSnapshot.position_key,
            PositionSnapshot.wallet_id,
            Wallet.wallet_type,
            WalletProductMap.product_id,
            Market.protocol_id,
            Protocol.protocol_code,
            Market.chain_id,
            PositionSnapshot.market_id,
            Market.market_kind,
            Market.display_name,
            Market.metadata_json,
            Market.base_asset_token_id,
            Market.collateral_token_id,
            PositionSnapshot.supplied_amount,
            PositionSnapshot.supplied_usd,
            PositionSnapshot.collateral_amount,
            PositionSnapshot.collateral_usd,
            PositionSnapshot.borrowed_amount,
            PositionSnapshot.borrowed_usd,
            PositionSnapshot.supply_apy,
            PositionSnapshot.reward_apy,
            PositionSnapshot.borrow_apy,
        )
        .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .outerjoin(WalletProductMap, WalletProductMap.wallet_id == PositionSnapshot.wallet_id)
        .order_by(PositionSnapshot.snapshot_id.asc())
    )
    if as_of_ts_utc is None:
        stmt = stmt.where(PositionSnapshot.position_id.is_(None))
    else:
        stmt = stmt.where(PositionSnapshot.as_of_ts_utc == as_of_ts_utc)
    rows = session.execute(stmt).all()
    return [
        _SnapshotDescriptor(
            snapshot_id=row.snapshot_id,
            as_of_ts_utc=row.as_of_ts_utc,
            position_key=row.position_key,
            wallet_id=row.wallet_id,
            wallet_type=row.wallet_type,
            product_id=row.product_id,
            protocol_id=row.protocol_id,
            protocol_code=row.protocol_code,
            chain_id=row.chain_id,
            market_id=row.market_id,
            market_kind=row.market_kind or "other",
            market_display_name=row.display_name or row.position_key,
            metadata_json=row.metadata_json if isinstance(row.metadata_json, dict) else None,
            base_asset_token_id=row.base_asset_token_id,
            collateral_token_id=row.collateral_token_id,
            supplied_amount=row.supplied_amount,
            supplied_usd=row.supplied_usd,
            collateral_amount=row.collateral_amount,
            collateral_usd=row.collateral_usd,
            borrowed_amount=row.borrowed_amount,
            borrowed_usd=row.borrowed_usd,
            supply_apy=row.supply_apy,
            reward_apy=row.reward_apy,
            borrow_apy=row.borrow_apy,
        )
        for row in rows
    ]


def backfill_positions_and_legs(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> PositionBackfillSummary:
    ensure_market_exposures(session)
    descriptors = _load_snapshot_descriptors(session, as_of_ts_utc=as_of_ts_utc)
    if not descriptors:
        return PositionBackfillSummary(positions_written=0, snapshots_linked=0, legs_written=0)

    exposure_rows = session.execute(
        select(MarketExposureComponent.market_id, MarketExposureComponent.market_exposure_id)
    ).all()
    exposure_ids = {
        market_id: market_exposure_id for market_id, market_exposure_id in exposure_rows
    }

    position_rows: dict[str, dict[str, object]] = {}
    leg_rows_by_snapshot: dict[int, list[dict[str, object]]] = {}

    for descriptor in descriptors:
        exposure_class = position_exposure_class(
            descriptor.metadata_json,
            descriptor.protocol_code,
        )
        supply_token_id: int | None
        if (
            descriptor.market_kind == "consumer_market"
            and descriptor.collateral_token_id is not None
        ):
            supply_token_id = descriptor.collateral_token_id
        else:
            supply_token_id = economic_supply_token_id(
                base_asset_token_id=descriptor.base_asset_token_id,
                collateral_token_id=descriptor.collateral_token_id,
                collateral_amount=descriptor.collateral_amount,
                collateral_usd=descriptor.collateral_usd,
            )
        if supply_token_id is None:
            if exposure_class != "core_lending":
                continue
            continue
        borrow_token_id = descriptor.base_asset_token_id
        if descriptor.borrowed_amount > ZERO and borrow_token_id is None:
            if exposure_class != "core_lending":
                continue
            continue

        position_rows.setdefault(
            descriptor.position_key,
            {
                "position_key": descriptor.position_key,
                "wallet_id": descriptor.wallet_id,
                "product_id": descriptor.product_id,
                "protocol_id": descriptor.protocol_id,
                "chain_id": descriptor.chain_id,
                "market_id": descriptor.market_id,
                "market_exposure_id": exposure_ids.get(descriptor.market_id),
                "exposure_class": exposure_class,
                "status": "open",
                "display_name": descriptor.market_display_name,
                "opened_at_utc": descriptor.as_of_ts_utc,
                "last_seen_at_utc": descriptor.as_of_ts_utc,
            },
        )

        supply_amount = economic_supply_amount(
            supplied_amount=descriptor.supplied_amount,
            collateral_amount=descriptor.collateral_amount,
            collateral_token_id=descriptor.collateral_token_id,
            collateral_usd=descriptor.collateral_usd,
        )
        supply_usd = economic_supply_usd(
            supplied_usd=descriptor.supplied_usd,
            collateral_usd=descriptor.collateral_usd,
            collateral_token_id=descriptor.collateral_token_id,
            collateral_amount=descriptor.collateral_amount,
        )
        legs = [
            {
                "snapshot_id": descriptor.snapshot_id,
                "leg_type": "supply",
                "token_id": supply_token_id,
                "market_id": descriptor.market_id,
                "amount_native": supply_amount,
                "usd_value": supply_usd,
                "rate": descriptor.supply_apy + descriptor.reward_apy,
                "estimated_daily_cashflow_usd": supply_usd
                * (descriptor.supply_apy + descriptor.reward_apy)
                / DAYS_PER_YEAR,
                "is_collateral": supply_token_id == descriptor.collateral_token_id,
            }
        ]
        if descriptor.borrowed_amount > ZERO and borrow_token_id is not None:
            legs.append(
                {
                    "snapshot_id": descriptor.snapshot_id,
                    "leg_type": "borrow",
                    "token_id": borrow_token_id,
                    "market_id": descriptor.market_id,
                    "amount_native": descriptor.borrowed_amount,
                    "usd_value": descriptor.borrowed_usd,
                    "rate": descriptor.borrow_apy,
                    "estimated_daily_cashflow_usd": -(
                        descriptor.borrowed_usd * descriptor.borrow_apy / DAYS_PER_YEAR
                    ),
                    "is_collateral": False,
                }
            )
        leg_rows_by_snapshot[descriptor.snapshot_id] = legs

    if not position_rows:
        return PositionBackfillSummary(positions_written=0, snapshots_linked=0, legs_written=0)

    stmt = insert(Position).values(list(position_rows.values()))
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
    session.execute(stmt)

    position_id_rows = session.execute(select(Position.position_key, Position.position_id)).all()
    position_ids = {position_key: position_id for position_key, position_id in position_id_rows}

    snapshots_linked = 0
    for descriptor in descriptors:
        position_id = position_ids.get(descriptor.position_key)
        if position_id is None:
            continue
        session.execute(
            update(PositionSnapshot)
            .where(PositionSnapshot.snapshot_id == descriptor.snapshot_id)
            .values(position_id=position_id)
        )
        snapshots_linked += 1

    snapshot_ids = list(leg_rows_by_snapshot)
    if snapshot_ids:
        session.execute(
            delete(PositionSnapshotLeg).where(PositionSnapshotLeg.snapshot_id.in_(snapshot_ids))
        )
    leg_rows = [row for rows in leg_rows_by_snapshot.values() for row in rows]
    if leg_rows:
        leg_stmt = insert(PositionSnapshotLeg).values(leg_rows)
        leg_stmt = leg_stmt.on_conflict_do_update(
            index_elements=[PositionSnapshotLeg.snapshot_id, PositionSnapshotLeg.leg_type],
            set_={
                "token_id": leg_stmt.excluded.token_id,
                "market_id": leg_stmt.excluded.market_id,
                "amount_native": leg_stmt.excluded.amount_native,
                "usd_value": leg_stmt.excluded.usd_value,
                "rate": leg_stmt.excluded.rate,
                "estimated_daily_cashflow_usd": leg_stmt.excluded.estimated_daily_cashflow_usd,
                "is_collateral": leg_stmt.excluded.is_collateral,
            },
        )
        session.execute(leg_stmt)

    return PositionBackfillSummary(
        positions_written=len(position_rows),
        snapshots_linked=snapshots_linked,
        legs_written=len(leg_rows),
    )
