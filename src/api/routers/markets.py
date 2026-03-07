"""Served market exposure endpoints for the executive dashboard."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from api.deps import get_session
from api.schemas.alerts import AlertRow
from api.schemas.markets import (
    MarketExposureDetailResponse,
    MarketExposureHistoryPoint,
    MarketExposureRow,
    MarketSummaryResponse,
    NativeMarketComponent,
    NativeMarketDetailResponse,
)
from core.dashboard_contracts import alert_severity_label, alert_status_label
from core.db.models import (
    Alert,
    Chain,
    Market,
    MarketExposure,
    MarketExposureComponent,
    MarketExposureDaily,
    MarketHealthDaily,
    MarketSummaryDaily,
    Protocol,
    Token,
)

router = APIRouter(prefix="/markets")

SUPPLY_TOKEN = aliased(Token)
DEBT_TOKEN = aliased(Token)
COLLATERAL_TOKEN = aliased(Token)
BASE_TOKEN = aliased(Token)
MARKET_COLLATERAL_TOKEN = aliased(Token)
ACTIVE_ALERT_STATUSES = ("open", "ack")


def _alert_row(alert: Alert) -> AlertRow:
    return AlertRow(
        alert_id=alert.alert_id,
        ts_utc=alert.ts_utc,
        alert_type=alert.alert_type,
        alert_type_label=alert.alert_type.replace("_", " ").title(),
        severity=alert.severity,
        severity_label=alert_severity_label(alert.severity),
        entity_type=alert.entity_type,
        entity_id=alert.entity_id,
        payload_json=alert.payload_json,
        status=alert.status,
        status_label=alert_status_label(alert.status),
    )


def _latest_business_date(session: Session) -> date | None:
    return session.scalar(select(func.max(MarketExposureDaily.business_date)))


def _exposure_row_query(business_date: date):
    return (
        select(
            MarketExposure.market_exposure_id,
            MarketExposure.exposure_slug,
            MarketExposure.display_name,
            Protocol.protocol_code,
            Chain.chain_code,
            SUPPLY_TOKEN.symbol.label("supply_symbol"),
            DEBT_TOKEN.symbol.label("debt_symbol"),
            COLLATERAL_TOKEN.symbol.label("collateral_symbol"),
            MarketExposureDaily.total_supply_usd,
            MarketExposureDaily.total_borrow_usd,
            MarketExposureDaily.weighted_supply_apy,
            MarketExposureDaily.weighted_borrow_apy,
            MarketExposureDaily.utilization,
            MarketExposureDaily.available_liquidity_usd,
            MarketExposureDaily.distance_to_kink,
            MarketExposureDaily.strategy_position_count,
            MarketExposureDaily.customer_position_count,
            MarketExposureDaily.active_alert_count,
            MarketExposureDaily.risk_status,
            MarketExposureDaily.watch_status,
        )
        .join(
            MarketExposure,
            MarketExposure.market_exposure_id == MarketExposureDaily.market_exposure_id,
        )
        .join(Protocol, Protocol.protocol_id == MarketExposure.protocol_id)
        .join(Chain, Chain.chain_id == MarketExposure.chain_id)
        .outerjoin(SUPPLY_TOKEN, SUPPLY_TOKEN.token_id == MarketExposure.supply_token_id)
        .outerjoin(DEBT_TOKEN, DEBT_TOKEN.token_id == MarketExposure.debt_token_id)
        .outerjoin(
            COLLATERAL_TOKEN, COLLATERAL_TOKEN.token_id == MarketExposure.collateral_token_id
        )
        .where(MarketExposureDaily.business_date == business_date)
    )


def _build_exposure_row(row: Any) -> MarketExposureRow:
    return MarketExposureRow(
        market_exposure_id=row.market_exposure_id,
        exposure_slug=row.exposure_slug,
        display_name=row.display_name,
        protocol_code=row.protocol_code,
        chain_code=row.chain_code,
        supply_symbol=row.supply_symbol,
        debt_symbol=row.debt_symbol,
        collateral_symbol=row.collateral_symbol,
        total_supply_usd=row.total_supply_usd,
        total_borrow_usd=row.total_borrow_usd,
        weighted_supply_apy=row.weighted_supply_apy,
        weighted_borrow_apy=row.weighted_borrow_apy,
        utilization=row.utilization,
        available_liquidity_usd=row.available_liquidity_usd,
        distance_to_kink=row.distance_to_kink,
        strategy_position_count=row.strategy_position_count,
        customer_position_count=row.customer_position_count,
        active_alert_count=row.active_alert_count,
        risk_status=row.risk_status,
        watch_status=row.watch_status,
    )


@router.get("/exposures")
def get_market_exposures(
    protocol_code: str | None = Query(default=None),
    chain_code: str | None = Query(default=None),
    watch_only: bool = Query(default=False),
    session: Session = Depends(get_session),
) -> list[MarketExposureRow]:
    business_date = _latest_business_date(session)
    if business_date is None:
        return []

    stmt = _exposure_row_query(business_date)
    if protocol_code is not None:
        stmt = stmt.where(Protocol.protocol_code == protocol_code)
    if chain_code is not None:
        stmt = stmt.where(Chain.chain_code == chain_code)
    if watch_only:
        stmt = stmt.where(MarketExposureDaily.watch_status != "normal")
    stmt = stmt.order_by(
        MarketExposureDaily.active_alert_count.desc(),
        MarketExposureDaily.total_borrow_usd.desc(),
        MarketExposure.display_name.asc(),
    )
    return [_build_exposure_row(row) for row in session.execute(stmt).all()]


@router.get("/exposures/{exposure_slug}")
def get_market_exposure_detail(
    exposure_slug: str,
    days: int = Query(default=30, ge=1, le=365),
    session: Session = Depends(get_session),
) -> MarketExposureDetailResponse:
    business_date = _latest_business_date(session)
    if business_date is None:
        raise HTTPException(status_code=404, detail="market exposure not found")

    exposure_row = session.execute(
        _exposure_row_query(business_date)
        .where(MarketExposure.exposure_slug == exposure_slug)
        .limit(1)
    ).first()
    if exposure_row is None:
        raise HTTPException(status_code=404, detail="market exposure not found")

    exposure = _build_exposure_row(exposure_row)
    latest_date = session.scalar(
        select(func.max(MarketExposureDaily.business_date))
        .join(
            MarketExposure,
            MarketExposure.market_exposure_id == MarketExposureDaily.market_exposure_id,
        )
        .where(MarketExposure.exposure_slug == exposure_slug)
    )
    history: list[MarketExposureHistoryPoint] = []
    if latest_date is not None:
        start_date = latest_date.fromordinal(latest_date.toordinal() - days + 1)
        history_rows = session.execute(
            select(
                MarketExposureDaily.business_date,
                MarketExposureDaily.total_supply_usd,
                MarketExposureDaily.total_borrow_usd,
                MarketExposureDaily.weighted_supply_apy,
                MarketExposureDaily.weighted_borrow_apy,
                MarketExposureDaily.utilization,
                MarketExposureDaily.available_liquidity_usd,
                MarketExposureDaily.distance_to_kink,
                MarketExposureDaily.active_alert_count,
                MarketExposureDaily.risk_status,
            )
            .join(
                MarketExposure,
                MarketExposure.market_exposure_id == MarketExposureDaily.market_exposure_id,
            )
            .where(
                MarketExposure.exposure_slug == exposure_slug,
                MarketExposureDaily.business_date >= start_date,
                MarketExposureDaily.business_date <= latest_date,
            )
            .order_by(MarketExposureDaily.business_date.asc())
        ).all()
        history = [
            MarketExposureHistoryPoint(
                business_date=row.business_date,
                total_supply_usd=row.total_supply_usd,
                total_borrow_usd=row.total_borrow_usd,
                weighted_supply_apy=row.weighted_supply_apy,
                weighted_borrow_apy=row.weighted_borrow_apy,
                utilization=row.utilization,
                available_liquidity_usd=row.available_liquidity_usd,
                distance_to_kink=row.distance_to_kink,
                active_alert_count=row.active_alert_count,
                risk_status=row.risk_status,
            )
            for row in history_rows
        ]

    component_rows = session.execute(
        select(
            Market.market_id,
            Market.display_name,
            Market.market_kind,
            Protocol.protocol_code,
            Chain.chain_code,
            BASE_TOKEN.symbol.label("base_asset_symbol"),
            MARKET_COLLATERAL_TOKEN.symbol.label("collateral_symbol"),
            MarketHealthDaily.total_supply_usd,
            MarketHealthDaily.total_borrow_usd,
            MarketHealthDaily.utilization,
            MarketHealthDaily.supply_apy,
            MarketHealthDaily.borrow_apy,
            MarketHealthDaily.available_liquidity_usd,
            MarketHealthDaily.distance_to_kink,
            MarketHealthDaily.active_alert_count,
        )
        .join(MarketExposureComponent, MarketExposureComponent.market_id == Market.market_id)
        .join(
            MarketExposure,
            MarketExposure.market_exposure_id == MarketExposureComponent.market_exposure_id,
        )
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(BASE_TOKEN, BASE_TOKEN.token_id == Market.base_asset_token_id)
        .outerjoin(
            MARKET_COLLATERAL_TOKEN, MARKET_COLLATERAL_TOKEN.token_id == Market.collateral_token_id
        )
        .outerjoin(
            MarketHealthDaily,
            (MarketHealthDaily.market_id == Market.market_id)
            & (MarketHealthDaily.business_date == business_date),
        )
        .where(MarketExposure.exposure_slug == exposure_slug)
        .order_by(Market.market_id.asc())
    ).all()

    components = [
        NativeMarketComponent(
            market_id=row.market_id,
            display_name=row.display_name,
            market_kind=row.market_kind,
            protocol_code=row.protocol_code,
            chain_code=row.chain_code,
            base_asset_symbol=row.base_asset_symbol,
            collateral_symbol=row.collateral_symbol,
            current_total_supply_usd=row.total_supply_usd,
            current_total_borrow_usd=row.total_borrow_usd,
            current_utilization=row.utilization,
            current_supply_apy=row.supply_apy,
            current_borrow_apy=row.borrow_apy,
            current_available_liquidity_usd=row.available_liquidity_usd,
            current_distance_to_kink=row.distance_to_kink,
            active_alert_count=row.active_alert_count or 0,
        )
        for row in component_rows
    ]

    market_ids = [row.market_id for row in component_rows]
    alerts = []
    if market_ids:
        alert_rows = session.scalars(
            select(Alert)
            .where(
                Alert.entity_type == "market",
                Alert.status.in_(ACTIVE_ALERT_STATUSES),
                Alert.entity_id.in_([str(market_id) for market_id in market_ids]),
            )
            .order_by(Alert.ts_utc.desc())
        ).all()
        alerts = [_alert_row(alert) for alert in alert_rows]

    return MarketExposureDetailResponse(
        exposure=exposure,
        history=history,
        components=components,
        alerts=alerts,
    )


@router.get("/native/{market_id}")
def get_native_market_detail(
    market_id: int,
    days: int = Query(default=30, ge=1, le=365),
    session: Session = Depends(get_session),
) -> NativeMarketDetailResponse:
    market = session.get(Market, market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="market not found")

    business_date = session.scalar(
        select(func.max(MarketHealthDaily.business_date)).where(
            MarketHealthDaily.market_id == market_id
        )
    )

    component_row = session.execute(
        select(
            Market.market_id,
            Market.display_name,
            Market.market_kind,
            Protocol.protocol_code,
            Chain.chain_code,
            BASE_TOKEN.symbol.label("base_asset_symbol"),
            MARKET_COLLATERAL_TOKEN.symbol.label("collateral_symbol"),
            MarketHealthDaily.total_supply_usd,
            MarketHealthDaily.total_borrow_usd,
            MarketHealthDaily.utilization,
            MarketHealthDaily.supply_apy,
            MarketHealthDaily.borrow_apy,
            MarketHealthDaily.available_liquidity_usd,
            MarketHealthDaily.distance_to_kink,
            MarketHealthDaily.active_alert_count,
        )
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(BASE_TOKEN, BASE_TOKEN.token_id == Market.base_asset_token_id)
        .outerjoin(
            MARKET_COLLATERAL_TOKEN, MARKET_COLLATERAL_TOKEN.token_id == Market.collateral_token_id
        )
        .outerjoin(
            MarketHealthDaily,
            (MarketHealthDaily.market_id == Market.market_id)
            & (MarketHealthDaily.business_date == business_date),
        )
        .where(Market.market_id == market_id)
    ).first()
    if component_row is None:
        raise HTTPException(status_code=404, detail="market not found")

    component = NativeMarketComponent(
        market_id=component_row.market_id,
        display_name=component_row.display_name,
        market_kind=component_row.market_kind,
        protocol_code=component_row.protocol_code,
        chain_code=component_row.chain_code,
        base_asset_symbol=component_row.base_asset_symbol,
        collateral_symbol=component_row.collateral_symbol,
        current_total_supply_usd=component_row.total_supply_usd,
        current_total_borrow_usd=component_row.total_borrow_usd,
        current_utilization=component_row.utilization,
        current_supply_apy=component_row.supply_apy,
        current_borrow_apy=component_row.borrow_apy,
        current_available_liquidity_usd=component_row.available_liquidity_usd,
        current_distance_to_kink=component_row.distance_to_kink,
        active_alert_count=component_row.active_alert_count or 0,
    )

    history: list[MarketExposureHistoryPoint] = []
    if business_date is not None:
        start_date = business_date.fromordinal(business_date.toordinal() - days + 1)
        history_rows = session.execute(
            select(
                MarketHealthDaily.business_date,
                MarketHealthDaily.total_supply_usd,
                MarketHealthDaily.total_borrow_usd,
                MarketHealthDaily.supply_apy,
                MarketHealthDaily.borrow_apy,
                MarketHealthDaily.utilization,
                MarketHealthDaily.available_liquidity_usd,
                MarketHealthDaily.distance_to_kink,
                MarketHealthDaily.active_alert_count,
                MarketHealthDaily.risk_status,
            )
            .where(
                MarketHealthDaily.market_id == market_id,
                MarketHealthDaily.business_date >= start_date,
                MarketHealthDaily.business_date <= business_date,
            )
            .order_by(MarketHealthDaily.business_date.asc())
        ).all()
        history = [
            MarketExposureHistoryPoint(
                business_date=row.business_date,
                total_supply_usd=row.total_supply_usd,
                total_borrow_usd=row.total_borrow_usd,
                weighted_supply_apy=row.supply_apy,
                weighted_borrow_apy=row.borrow_apy,
                utilization=row.utilization,
                available_liquidity_usd=row.available_liquidity_usd,
                distance_to_kink=row.distance_to_kink,
                active_alert_count=row.active_alert_count,
                risk_status=row.risk_status,
            )
            for row in history_rows
        ]

    return NativeMarketDetailResponse(component=component, history=history)


@router.get("/summary")
def get_market_summary(session: Session = Depends(get_session)) -> MarketSummaryResponse:
    business_date = session.scalar(select(func.max(MarketSummaryDaily.business_date)))
    if business_date is None:
        today = datetime.now(UTC).date()
        return MarketSummaryResponse(
            business_date=today,
            scope_segment="strategy_only",
            total_supply_usd=Decimal("0"),
            total_borrow_usd=Decimal("0"),
            weighted_utilization=None,
            total_available_liquidity_usd=Decimal("0"),
            markets_at_risk_count=0,
            markets_on_watchlist_count=0,
        )

    row = session.scalar(
        select(MarketSummaryDaily).where(
            MarketSummaryDaily.business_date == business_date,
            MarketSummaryDaily.scope_segment == "strategy_only",
        )
    )
    if row is None:
        raise HTTPException(status_code=404, detail="market summary not found")

    return MarketSummaryResponse(
        business_date=row.business_date,
        scope_segment=row.scope_segment,
        total_supply_usd=row.total_supply_usd,
        total_borrow_usd=row.total_borrow_usd,
        weighted_utilization=row.weighted_utilization,
        total_available_liquidity_usd=row.total_available_liquidity_usd,
        markets_at_risk_count=row.markets_at_risk_count,
        markets_on_watchlist_count=row.markets_on_watchlist_count,
    )
