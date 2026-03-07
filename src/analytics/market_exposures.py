"""Helpers for building the dashboard-facing market exposure dimension."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased

from core.dashboard_contracts import (
    market_display_name,
    market_exposure_display_name,
    market_exposure_kind,
    market_exposure_slug,
    market_exposure_tokens,
    market_kind,
)
from core.db.models import Chain, Market, MarketExposure, MarketExposureComponent, Protocol, Token


@dataclass(frozen=True)
class MarketExposureBuildSummary:
    exposures_written: int
    components_written: int


@dataclass(frozen=True)
class MarketExposureDescriptor:
    market_id: int
    market_exposure_slug: str
    protocol_id: int
    chain_id: int
    exposure_kind: str
    supply_token_id: int | None
    debt_token_id: int | None
    collateral_token_id: int | None
    display_name: str


BASE_TOKEN = aliased(Token)
COLLATERAL_TOKEN = aliased(Token)


def build_market_exposure_descriptors(session: Session) -> list[MarketExposureDescriptor]:
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

    descriptors: list[MarketExposureDescriptor] = []
    for row in rows:
        metadata_json: dict[str, Any] | None = (
            row.metadata_json if isinstance(row.metadata_json, dict) else None
        )
        kind = row.market_kind or market_kind(metadata_json)
        supply_token_id, debt_token_id, collateral_token_id = market_exposure_tokens(
            market_kind_value=kind,
            base_token_id=row.base_asset_token_id,
            collateral_token_id=row.collateral_token_id,
        )
        market_label = row.display_name or market_display_name(
            protocol_code=row.protocol_code,
            base_symbol=row.base_symbol,
            collateral_symbol=row.collateral_symbol,
            metadata_json=metadata_json,
            market_address=row.market_address,
        )
        supply_symbol = (
            row.collateral_symbol
            if collateral_token_id and collateral_token_id != row.base_asset_token_id
            else row.base_symbol
        )
        debt_symbol = row.base_symbol if debt_token_id is not None else None
        exposure_label = market_exposure_display_name(
            market_kind_value=kind,
            supply_symbol=supply_symbol,
            debt_symbol=debt_symbol,
            market_display=market_label,
        )
        descriptors.append(
            MarketExposureDescriptor(
                market_id=row.market_id,
                market_exposure_slug=market_exposure_slug(
                    protocol_code=row.protocol_code,
                    chain_code=row.chain_code,
                    display_name=exposure_label,
                ),
                protocol_id=row.protocol_id,
                chain_id=row.chain_id,
                exposure_kind=market_exposure_kind(
                    market_kind_value=kind,
                    base_token_id=row.base_asset_token_id,
                    collateral_token_id=row.collateral_token_id,
                ),
                supply_token_id=supply_token_id,
                debt_token_id=debt_token_id,
                collateral_token_id=collateral_token_id,
                display_name=exposure_label,
            )
        )
    return descriptors


def ensure_market_exposures(session: Session) -> MarketExposureBuildSummary:
    descriptors = build_market_exposure_descriptors(session)
    if not descriptors:
        return MarketExposureBuildSummary(exposures_written=0, components_written=0)

    exposure_rows: dict[str, dict[str, object]] = {}
    component_rows: list[dict[str, object]] = []
    for descriptor in descriptors:
        exposure_rows[descriptor.market_exposure_slug] = {
            "protocol_id": descriptor.protocol_id,
            "chain_id": descriptor.chain_id,
            "exposure_kind": descriptor.exposure_kind,
            "supply_token_id": descriptor.supply_token_id,
            "debt_token_id": descriptor.debt_token_id,
            "collateral_token_id": descriptor.collateral_token_id,
            "exposure_slug": descriptor.market_exposure_slug,
            "display_name": descriptor.display_name,
        }

    stmt = insert(MarketExposure).values(list(exposure_rows.values()))
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

    for descriptor in descriptors:
        market_exposure_id = exposure_ids.get(descriptor.market_exposure_slug)
        if market_exposure_id is None:
            continue
        component_rows.append(
            {
                "market_exposure_id": market_exposure_id,
                "market_id": descriptor.market_id,
                "component_role": "primary_market",
            }
        )

    components_written = 0
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
        components_written = len(component_rows)

    return MarketExposureBuildSummary(
        exposures_written=len(exposure_rows),
        components_written=components_written,
    )
