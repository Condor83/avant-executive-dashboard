"""Helpers for building the dashboard-facing market exposure dimension."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased

from core.dashboard_contracts import (
    market_display_name,
    market_exposure_display_name,
    market_exposure_kind,
    market_exposure_slug,
)
from core.db.models import (
    Chain,
    Market,
    MarketExposure,
    MarketExposureComponent,
    PositionSnapshot,
    Protocol,
    Token,
    WalletProductMap,
)

ZERO = Decimal("0")
DUST_BORROW_USD = Decimal("1")
EXCLUDED_PROTOCOLS = {"wallet_balances", "traderjoe_lp", "stakedao", "etherex"}
RESERVE_USAGE_PROTOCOLS = {"aave_v3", "spark"}
ACCOUNT_GROUPED_PROTOCOLS = {"dolomite"}
PRODUCT_GROUPED_PROTOCOLS = {"zest"}
LIVE_NATIVE_MARKET_KINDS = {"market", "consumer_market"}


@dataclass(frozen=True)
class MarketExposureBuildSummary:
    exposures_written: int
    components_written: int


@dataclass(frozen=True)
class MarketExposureDescriptor:
    market_exposure_slug: str
    protocol_id: int
    chain_id: int
    exposure_kind: str
    supply_token_id: int | None
    debt_token_id: int | None
    collateral_token_id: int | None
    display_name: str
    monitored: bool
    strategy_position_count: int
    component_roles: tuple[tuple[int, str], ...]


@dataclass(frozen=True)
class _MarketRecord:
    market_id: int
    protocol_id: int
    protocol_code: str
    chain_id: int
    chain_code: str
    market_kind: str
    market_address: str
    display_name: str
    base_asset_token_id: int | None
    collateral_token_id: int | None
    base_symbol: str | None
    collateral_symbol: str | None
    metadata_json: dict[str, Any] | None


@dataclass(frozen=True)
class _LivePositionRow:
    position_key: str
    wallet_id: int
    product_id: int | None
    protocol_id: int
    protocol_code: str
    chain_id: int
    chain_code: str
    market_id: int
    supplied_usd: Decimal
    collateral_usd: Decimal | None
    borrowed_usd: Decimal
    supply_apy: Decimal
    reward_apy: Decimal


@dataclass
class _ExposureState:
    market_exposure_slug: str
    protocol_id: int
    chain_id: int
    exposure_kind: str
    supply_token_id: int | None
    debt_token_id: int | None
    collateral_token_id: int | None
    display_name: str
    monitored: bool = False
    usage_keys: set[str] = field(default_factory=set)
    component_roles: set[tuple[int, str]] = field(default_factory=set)
    borrow_usd: Decimal = ZERO
    collateral_yield_weighted_usd: Decimal = ZERO
    collateral_yield_weighted_sum: Decimal = ZERO


BASE_TOKEN = aliased(Token)
COLLATERAL_TOKEN = aliased(Token)


def _load_market_records(session: Session) -> tuple[list[_MarketRecord], dict[int, _MarketRecord]]:
    rows = session.execute(
        select(
            Market.market_id,
            Market.protocol_id,
            Protocol.protocol_code,
            Market.chain_id,
            Chain.chain_code,
            Market.market_kind,
            Market.market_address,
            Market.display_name,
            Market.base_asset_token_id,
            Market.collateral_token_id,
            BASE_TOKEN.symbol.label("base_symbol"),
            COLLATERAL_TOKEN.symbol.label("collateral_symbol"),
            Market.metadata_json,
        )
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(BASE_TOKEN, BASE_TOKEN.token_id == Market.base_asset_token_id)
        .outerjoin(COLLATERAL_TOKEN, COLLATERAL_TOKEN.token_id == Market.collateral_token_id)
        .order_by(Market.market_id.asc())
    ).all()

    records = [
        _MarketRecord(
            market_id=row.market_id,
            protocol_id=row.protocol_id,
            protocol_code=row.protocol_code,
            chain_id=row.chain_id,
            chain_code=row.chain_code,
            market_kind=row.market_kind or "other",
            market_address=row.market_address,
            display_name=row.display_name,
            base_asset_token_id=row.base_asset_token_id,
            collateral_token_id=row.collateral_token_id,
            base_symbol=row.base_symbol,
            collateral_symbol=row.collateral_symbol,
            metadata_json=row.metadata_json if isinstance(row.metadata_json, dict) else None,
        )
        for row in rows
    ]
    by_id = {record.market_id: record for record in records}
    return records, by_id


def _load_live_position_rows(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> list[_LivePositionRow]:
    snapshot_ts = as_of_ts_utc
    if snapshot_ts is None:
        snapshot_ts = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
    if snapshot_ts is None:
        return []
    rows = session.execute(
        select(
            PositionSnapshot.position_key,
            PositionSnapshot.wallet_id,
            WalletProductMap.product_id,
            Market.protocol_id,
            Protocol.protocol_code,
            Market.chain_id,
            Chain.chain_code,
            PositionSnapshot.market_id,
            PositionSnapshot.supplied_usd,
            PositionSnapshot.collateral_usd,
            PositionSnapshot.borrowed_usd,
            PositionSnapshot.supply_apy,
            PositionSnapshot.reward_apy,
        )
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(WalletProductMap, WalletProductMap.wallet_id == PositionSnapshot.wallet_id)
        .where(PositionSnapshot.as_of_ts_utc == snapshot_ts)
        .order_by(PositionSnapshot.snapshot_id.asc())
    ).all()
    return [
        _LivePositionRow(
            position_key=row.position_key,
            wallet_id=row.wallet_id,
            product_id=row.product_id,
            protocol_id=row.protocol_id,
            protocol_code=row.protocol_code,
            chain_id=row.chain_id,
            chain_code=row.chain_code,
            market_id=row.market_id,
            supplied_usd=row.supplied_usd,
            collateral_usd=row.collateral_usd,
            borrowed_usd=row.borrowed_usd,
            supply_apy=row.supply_apy,
            reward_apy=row.reward_apy,
        )
        for row in rows
    ]


def _economic_supply_usd(row: _LivePositionRow) -> Decimal:
    collateral_usd = row.collateral_usd or ZERO
    if collateral_usd > ZERO:
        return collateral_usd
    return row.supplied_usd


def _build_market_label(record: _MarketRecord) -> str:
    return record.display_name or market_display_name(
        protocol_code=record.protocol_code,
        base_symbol=record.base_symbol,
        collateral_symbol=record.collateral_symbol,
        metadata_json=record.metadata_json,
        market_address=record.market_address,
    )


def _direct_supply_token_id(record: _MarketRecord) -> int | None:
    if (
        record.collateral_token_id is not None
        and record.collateral_token_id != record.base_asset_token_id
    ):
        return record.collateral_token_id
    return record.base_asset_token_id


def _direct_supply_symbol(record: _MarketRecord) -> str | None:
    if (
        record.collateral_token_id is not None
        and record.collateral_token_id != record.base_asset_token_id
    ):
        return record.collateral_symbol
    return record.base_symbol


def _upsert_state(
    states: dict[str, _ExposureState],
    *,
    protocol_id: int,
    protocol_code: str,
    chain_id: int,
    chain_code: str,
    supply_token_id: int | None,
    debt_token_id: int | None,
    collateral_token_id: int | None,
    supply_symbol: str | None,
    debt_symbol: str | None,
    market_display: str,
    market_kind_value: str,
    monitored: bool,
    usage_key: str | None,
    components: list[tuple[int, str]],
    borrow_usd: Decimal = ZERO,
    collateral_yield_apy: Decimal | None = None,
    collateral_yield_weight_usd: Decimal = ZERO,
) -> None:
    exposure_kind = market_exposure_kind(
        market_kind_value=market_kind_value,
        base_token_id=debt_token_id,
        collateral_token_id=collateral_token_id,
    )
    display_name = market_exposure_display_name(
        market_kind_value=market_kind_value,
        supply_symbol=supply_symbol,
        debt_symbol=debt_symbol,
        market_display=market_display,
    )
    slug = market_exposure_slug(
        protocol_code=protocol_code,
        chain_code=chain_code,
        display_name=display_name,
    )
    state = states.get(slug)
    if state is None:
        state = _ExposureState(
            market_exposure_slug=slug,
            protocol_id=protocol_id,
            chain_id=chain_id,
            exposure_kind=exposure_kind,
            supply_token_id=supply_token_id,
            debt_token_id=debt_token_id,
            collateral_token_id=collateral_token_id,
            display_name=display_name,
        )
        states[slug] = state
    state.monitored = state.monitored or monitored
    if usage_key is not None:
        state.usage_keys.add(usage_key)
    state.component_roles.update(components)
    state.borrow_usd += borrow_usd
    if collateral_yield_apy is not None and collateral_yield_weight_usd > ZERO:
        state.collateral_yield_weighted_sum += collateral_yield_apy * collateral_yield_weight_usd
        state.collateral_yield_weighted_usd += collateral_yield_weight_usd


def _component_address_parts(record: _MarketRecord) -> tuple[str, str] | None:
    if "/" not in record.market_address:
        return None
    left, right = record.market_address.split("/", 1)
    if not left or not right:
        return None
    return left, right


def _market_by_address(
    records: list[_MarketRecord],
) -> dict[tuple[int, int, str], list[_MarketRecord]]:
    grouped: dict[tuple[int, int, str], list[_MarketRecord]] = {}
    for record in records:
        grouped.setdefault((record.protocol_id, record.chain_id, record.market_address), []).append(
            record
        )
    return grouped


def _resolve_component_market(
    market_index: dict[tuple[int, int, str], list[_MarketRecord]],
    *,
    protocol_id: int,
    chain_id: int,
    market_address: str,
) -> _MarketRecord | None:
    matches = market_index.get((protocol_id, chain_id, market_address), [])
    if not matches:
        return None
    non_consumer = [record for record in matches if record.market_kind != "consumer_market"]
    if non_consumer:
        return non_consumer[0]
    return matches[0]


def _iter_direct_live_states(
    *,
    states: dict[str, _ExposureState],
    records_by_id: dict[int, _MarketRecord],
    live_rows: list[_LivePositionRow],
) -> None:
    for row in live_rows:
        record = records_by_id.get(row.market_id)
        if record is None:
            continue
        if record.protocol_code in EXCLUDED_PROTOCOLS:
            continue
        if record.protocol_code in (
            ACCOUNT_GROUPED_PROTOCOLS | PRODUCT_GROUPED_PROTOCOLS | RESERVE_USAGE_PROTOCOLS
        ):
            continue
        if record.market_kind not in LIVE_NATIVE_MARKET_KINDS:
            continue
        if (
            row.supplied_usd <= ZERO
            and (row.collateral_usd or ZERO) <= ZERO
            and row.borrowed_usd <= ZERO
        ):
            continue

        supply_token_id = _direct_supply_token_id(record)
        debt_token_id = (
            record.base_asset_token_id if supply_token_id != record.base_asset_token_id else None
        )
        if supply_token_id is None:
            continue
        if debt_token_id is None and record.market_kind == "consumer_market":
            continue
        _upsert_state(
            states,
            protocol_id=record.protocol_id,
            protocol_code=record.protocol_code,
            chain_id=record.chain_id,
            chain_code=record.chain_code,
            supply_token_id=supply_token_id,
            debt_token_id=debt_token_id,
            collateral_token_id=record.collateral_token_id,
            supply_symbol=_direct_supply_symbol(record),
            debt_symbol=record.base_symbol if debt_token_id is not None else None,
            market_display=_build_market_label(record),
            market_kind_value=record.market_kind,
            monitored=False,
            usage_key=row.position_key,
            components=[(record.market_id, "primary_market")],
            borrow_usd=row.borrowed_usd,
            collateral_yield_apy=row.supply_apy + row.reward_apy,
            collateral_yield_weight_usd=_economic_supply_usd(row),
        )


def _pair_usage_choices(
    supply_rows: list[_LivePositionRow],
    borrow_rows: list[_LivePositionRow],
    records_by_id: dict[int, _MarketRecord],
) -> list[tuple[_LivePositionRow, _LivePositionRow]]:
    pairs: list[tuple[_LivePositionRow, _LivePositionRow]] = []
    for supply_row in supply_rows:
        supply_record = records_by_id.get(supply_row.market_id)
        if supply_record is None or supply_record.base_asset_token_id is None:
            continue
        for borrow_row in borrow_rows:
            borrow_record = records_by_id.get(borrow_row.market_id)
            if borrow_record is None or borrow_record.base_asset_token_id is None:
                continue
            pairs.append((supply_row, borrow_row))
    if not pairs:
        return []
    non_same = [
        pair
        for pair in pairs
        if records_by_id[pair[0].market_id].base_asset_token_id
        != records_by_id[pair[1].market_id].base_asset_token_id
    ]
    return non_same or pairs


def _iter_bucketed_live_states(
    *,
    states: dict[str, _ExposureState],
    records_by_id: dict[int, _MarketRecord],
    live_rows: list[_LivePositionRow],
) -> None:
    reserve_buckets: dict[tuple[int, int | None, int, int], list[_LivePositionRow]] = {}
    dolomite_buckets: dict[tuple[int, int | None, int, int, str], list[_LivePositionRow]] = {}
    zest_buckets: dict[tuple[int, int | None, int, int], list[_LivePositionRow]] = {}

    for row in live_rows:
        record = records_by_id.get(row.market_id)
        if record is None:
            continue
        if record.protocol_code in RESERVE_USAGE_PROTOCOLS and record.market_kind == "reserve":
            reserve_buckets.setdefault(
                (row.wallet_id, row.product_id, row.protocol_id, row.chain_id), []
            ).append(row)
            continue
        if record.protocol_code in ACCOUNT_GROUPED_PROTOCOLS:
            account_number = _dolomite_account_number(row.position_key)
            if account_number is not None:
                dolomite_buckets.setdefault(
                    (row.wallet_id, row.product_id, row.protocol_id, row.chain_id, account_number),
                    [],
                ).append(row)
            continue
        if record.protocol_code in PRODUCT_GROUPED_PROTOCOLS:
            zest_buckets.setdefault(
                (row.wallet_id, row.product_id, row.protocol_id, row.chain_id), []
            ).append(row)

    for bucket in reserve_buckets.values():
        supply_rows = [row for row in bucket if row.supplied_usd > ZERO]
        borrow_rows = [row for row in bucket if row.borrowed_usd > ZERO]
        for supply_row, borrow_row in _pair_usage_choices(supply_rows, borrow_rows, records_by_id):
            supply_record = records_by_id[supply_row.market_id]
            borrow_record = records_by_id[borrow_row.market_id]
            usage_key = (
                f"{supply_row.wallet_id}:{supply_row.product_id}:{supply_row.protocol_id}:"
                f"{supply_row.chain_id}:{supply_record.base_asset_token_id}:"
                f"{borrow_record.base_asset_token_id}"
            )
            _upsert_state(
                states,
                protocol_id=supply_record.protocol_id,
                protocol_code=supply_record.protocol_code,
                chain_id=supply_record.chain_id,
                chain_code=supply_record.chain_code,
                supply_token_id=supply_record.base_asset_token_id,
                debt_token_id=borrow_record.base_asset_token_id,
                collateral_token_id=supply_record.base_asset_token_id,
                supply_symbol=supply_record.base_symbol,
                debt_symbol=borrow_record.base_symbol,
                market_display=f"{supply_record.base_symbol} / {borrow_record.base_symbol}",
                market_kind_value="consumer_market",
                monitored=False,
                usage_key=usage_key,
                components=[
                    (supply_record.market_id, "supply_market"),
                    (borrow_record.market_id, "borrow_market"),
                ],
                borrow_usd=borrow_row.borrowed_usd,
                collateral_yield_apy=supply_row.supply_apy + supply_row.reward_apy,
                collateral_yield_weight_usd=_economic_supply_usd(supply_row),
            )

    for bucket in dolomite_buckets.values():
        supply_rows = [row for row in bucket if row.supplied_usd > ZERO]
        borrow_rows = [row for row in bucket if row.borrowed_usd > ZERO]
        if not supply_rows or not borrow_rows:
            continue
        supply_row = max(supply_rows, key=lambda row: (row.supplied_usd, row.position_key))
        dolomite_supply_record = records_by_id.get(supply_row.market_id)
        if dolomite_supply_record is None:
            continue
        for _ignored_supply_row, borrow_row in _pair_usage_choices(
            [supply_row], borrow_rows, records_by_id
        ):
            borrow_record = records_by_id[borrow_row.market_id]
            usage_key = (
                f"{supply_row.wallet_id}:{supply_row.product_id}:{supply_row.protocol_id}:"
                f"{supply_row.chain_id}:{_dolomite_account_number(supply_row.position_key)}:"
                f"{borrow_record.base_asset_token_id}"
            )
            _upsert_state(
                states,
                protocol_id=dolomite_supply_record.protocol_id,
                protocol_code=dolomite_supply_record.protocol_code,
                chain_id=dolomite_supply_record.chain_id,
                chain_code=dolomite_supply_record.chain_code,
                supply_token_id=dolomite_supply_record.base_asset_token_id,
                debt_token_id=borrow_record.base_asset_token_id,
                collateral_token_id=dolomite_supply_record.base_asset_token_id,
                supply_symbol=dolomite_supply_record.base_symbol,
                debt_symbol=borrow_record.base_symbol,
                market_display=(
                    f"{dolomite_supply_record.base_symbol} / {borrow_record.base_symbol}"
                ),
                market_kind_value="consumer_market",
                monitored=False,
                usage_key=usage_key,
                components=[
                    (dolomite_supply_record.market_id, "supply_market"),
                    (borrow_record.market_id, "borrow_market"),
                ],
                borrow_usd=borrow_row.borrowed_usd,
                collateral_yield_apy=supply_row.supply_apy + supply_row.reward_apy,
                collateral_yield_weight_usd=_economic_supply_usd(supply_row),
            )

    for bucket in zest_buckets.values():
        supply_rows = [row for row in bucket if row.supplied_usd > ZERO]
        borrow_rows = [row for row in bucket if row.borrowed_usd > DUST_BORROW_USD]
        for supply_row, borrow_row in _pair_usage_choices(supply_rows, borrow_rows, records_by_id):
            supply_record = records_by_id[supply_row.market_id]
            borrow_record = records_by_id[borrow_row.market_id]
            usage_key = (
                f"{supply_row.wallet_id}:{supply_row.product_id}:{supply_row.protocol_id}:"
                f"{supply_row.chain_id}:{supply_record.base_asset_token_id}:"
                f"{borrow_record.base_asset_token_id}"
            )
            _upsert_state(
                states,
                protocol_id=supply_record.protocol_id,
                protocol_code=supply_record.protocol_code,
                chain_id=supply_record.chain_id,
                chain_code=supply_record.chain_code,
                supply_token_id=supply_record.base_asset_token_id,
                debt_token_id=borrow_record.base_asset_token_id,
                collateral_token_id=supply_record.base_asset_token_id,
                supply_symbol=supply_record.base_symbol,
                debt_symbol=borrow_record.base_symbol,
                market_display=f"{supply_record.base_symbol} / {borrow_record.base_symbol}",
                market_kind_value="consumer_market",
                monitored=False,
                usage_key=usage_key,
                components=[
                    (supply_record.market_id, "supply_market"),
                    (borrow_record.market_id, "borrow_market"),
                ],
                borrow_usd=borrow_row.borrowed_usd,
                collateral_yield_apy=supply_row.supply_apy + supply_row.reward_apy,
                collateral_yield_weight_usd=_economic_supply_usd(supply_row),
            )


def _iter_monitored_states(
    *,
    states: dict[str, _ExposureState],
    records: list[_MarketRecord],
) -> None:
    market_index = _market_by_address(records)
    for record in records:
        if record.market_kind != "consumer_market":
            continue
        if record.protocol_code in EXCLUDED_PROTOCOLS:
            continue
        if (
            record.base_asset_token_id is not None
            and record.base_asset_token_id == record.collateral_token_id
        ):
            continue

        components: list[tuple[int, str]] = []
        direct_component = _resolve_component_market(
            market_index,
            protocol_id=record.protocol_id,
            chain_id=record.chain_id,
            market_address=record.market_address,
        )
        use_self_primary = (
            direct_component is not None and direct_component.market_id == record.market_id
        )
        if use_self_primary:
            direct_component = None
        if direct_component is not None and direct_component.market_kind != "consumer_market":
            components.append((direct_component.market_id, "primary_market"))
        elif direct_component is not None:
            components.append((direct_component.market_id, "primary_market"))
        else:
            parts = _component_address_parts(record)
            if parts is not None:
                supply_market = _resolve_component_market(
                    market_index,
                    protocol_id=record.protocol_id,
                    chain_id=record.chain_id,
                    market_address=parts[0],
                )
                borrow_market = _resolve_component_market(
                    market_index,
                    protocol_id=record.protocol_id,
                    chain_id=record.chain_id,
                    market_address=parts[1],
                )
                if supply_market is not None:
                    components.append((supply_market.market_id, "supply_market"))
                if borrow_market is not None:
                    components.append((borrow_market.market_id, "borrow_market"))
        if not components and use_self_primary:
            components.append((record.market_id, "primary_market"))
        if not components:
            continue

        _upsert_state(
            states,
            protocol_id=record.protocol_id,
            protocol_code=record.protocol_code,
            chain_id=record.chain_id,
            chain_code=record.chain_code,
            supply_token_id=_direct_supply_token_id(record),
            debt_token_id=(
                record.base_asset_token_id
                if _direct_supply_token_id(record) != record.base_asset_token_id
                else None
            ),
            collateral_token_id=record.collateral_token_id,
            supply_symbol=_direct_supply_symbol(record),
            debt_symbol=(
                record.base_symbol
                if _direct_supply_token_id(record) != record.base_asset_token_id
                else None
            ),
            market_display=_build_market_label(record),
            market_kind_value=record.market_kind,
            monitored=True,
            usage_key=None,
            components=components,
        )


def _dolomite_account_number(position_key: str) -> str | None:
    parts = position_key.split(":", 4)
    if len(parts) != 5 or parts[0] != "dolomite":
        return None
    return parts[3]


def _build_exposure_states(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> dict[str, _ExposureState]:
    records, records_by_id = _load_market_records(session)
    live_rows = _load_live_position_rows(session, as_of_ts_utc=as_of_ts_utc)
    states: dict[str, _ExposureState] = {}
    _iter_monitored_states(states=states, records=records)
    _iter_direct_live_states(states=states, records_by_id=records_by_id, live_rows=live_rows)
    _iter_bucketed_live_states(states=states, records_by_id=records_by_id, live_rows=live_rows)
    return states


def build_market_exposure_descriptors(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> list[MarketExposureDescriptor]:
    states = _build_exposure_states(session, as_of_ts_utc=as_of_ts_utc)
    return [
        MarketExposureDescriptor(
            market_exposure_slug=state.market_exposure_slug,
            protocol_id=state.protocol_id,
            chain_id=state.chain_id,
            exposure_kind=state.exposure_kind,
            supply_token_id=state.supply_token_id,
            debt_token_id=state.debt_token_id,
            collateral_token_id=state.collateral_token_id,
            display_name=state.display_name,
            monitored=state.monitored,
            strategy_position_count=len(state.usage_keys),
            component_roles=tuple(sorted(state.component_roles)),
        )
        for state in sorted(states.values(), key=lambda item: item.market_exposure_slug)
    ]


def build_market_exposure_usage(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> dict[str, tuple[bool, int]]:
    metrics = build_market_exposure_usage_metrics(session, as_of_ts_utc=as_of_ts_utc)
    return {
        slug: (monitored, strategy_position_count)
        for slug, (
            monitored,
            strategy_position_count,
            _borrow_usd,
            _collateral_yield_apy,
        ) in metrics.items()
    }


def build_market_exposure_usage_metrics(
    session: Session,
    *,
    as_of_ts_utc: datetime | None = None,
) -> dict[str, tuple[bool, int, Decimal, Decimal | None]]:
    states = _build_exposure_states(session, as_of_ts_utc=as_of_ts_utc)
    return {
        state.market_exposure_slug: (
            state.monitored,
            len(state.usage_keys),
            state.borrow_usd,
            (
                state.collateral_yield_weighted_sum / state.collateral_yield_weighted_usd
                if state.collateral_yield_weighted_usd > ZERO
                else None
            ),
        )
        for state in states.values()
    }


def ensure_market_exposures(session: Session) -> MarketExposureBuildSummary:
    descriptors = build_market_exposure_descriptors(session)
    exposure_rows = [
        {
            "protocol_id": descriptor.protocol_id,
            "chain_id": descriptor.chain_id,
            "exposure_kind": descriptor.exposure_kind,
            "supply_token_id": descriptor.supply_token_id,
            "debt_token_id": descriptor.debt_token_id,
            "collateral_token_id": descriptor.collateral_token_id,
            "exposure_slug": descriptor.market_exposure_slug,
            "display_name": descriptor.display_name,
        }
        for descriptor in descriptors
    ]

    if exposure_rows:
        stmt = insert(MarketExposure).values(exposure_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[MarketExposure.exposure_slug],
            set_={
                "protocol_id": stmt.excluded.protocol_id,
                "chain_id": stmt.excluded.chain_id,
                "exposure_kind": stmt.excluded.exposure_kind,
                "supply_token_id": stmt.excluded.supply_token_id,
                "debt_token_id": stmt.excluded.debt_token_id,
                "collateral_token_id": stmt.excluded.collateral_token_id,
                "display_name": stmt.excluded.display_name,
            },
        )
        session.execute(stmt)

    exposure_id_rows = session.execute(
        select(MarketExposure.market_exposure_id, MarketExposure.exposure_slug)
    ).all()
    exposure_ids = {slug: market_exposure_id for market_exposure_id, slug in exposure_id_rows}

    component_rows: list[dict[str, object]] = []
    for descriptor in descriptors:
        market_exposure_id = exposure_ids.get(descriptor.market_exposure_slug)
        if market_exposure_id is None:
            continue
        for market_id, component_role in descriptor.component_roles:
            component_rows.append(
                {
                    "market_exposure_id": market_exposure_id,
                    "market_id": market_id,
                    "component_role": component_role,
                }
            )

    session.execute(delete(MarketExposureComponent))
    if component_rows:
        component_stmt = insert(MarketExposureComponent).values(component_rows)
        component_stmt = component_stmt.on_conflict_do_nothing(
            index_elements=[
                MarketExposureComponent.market_exposure_id,
                MarketExposureComponent.market_id,
                MarketExposureComponent.component_role,
            ]
        )
        session.execute(component_stmt)

    return MarketExposureBuildSummary(
        exposures_written=len(exposure_rows),
        components_written=len(component_rows),
    )
