"""Served market exposure endpoints for the executive dashboard."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, literal, select
from sqlalchemy.orm import Session, aliased

from analytics.market_exposures import build_market_exposure_usage_metrics
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
    MarketOverviewDaily,
    MarketSnapshot,
    MarketSummaryDaily,
    PortfolioPositionCurrent,
    Price,
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
ZERO = Decimal("0")
ONE = Decimal("1")


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


def _supply_component_token_id(component: Any) -> int | None:
    if (
        component.collateral_token_id is not None
        and component.collateral_token_id != component.base_asset_token_id
    ):
        return component.collateral_token_id
    return component.base_asset_token_id


def _pick_component(rows: list[Any], *roles: str) -> Any | None:
    for role in roles:
        for row in rows:
            if row.component_role == role:
                return row
    return rows[0] if rows else None


def _cap_usd(
    caps_json: dict[str, Any] | None,
    key: str,
    price_usd: Decimal | None,
) -> Decimal | None:
    if not isinstance(caps_json, dict) or price_usd is None:
        return None
    raw_value = caps_json.get(key)
    if raw_value is None:
        return None
    cap = Decimal(str(raw_value))
    if cap <= ONE:
        return None
    return cap * price_usd


def _load_latest_price_map(session: Session, token_ids: set[int]) -> dict[int, Decimal]:
    if not token_ids:
        return {}
    rows = session.execute(
        select(Price.token_id, Price.price_usd)
        .where(Price.token_id.in_(token_ids))
        .order_by(Price.token_id.asc(), Price.ts_utc.desc())
    ).all()
    prices: dict[int, Decimal] = {}
    for token_id, price_usd in rows:
        prices.setdefault(int(token_id), Decimal(str(price_usd)))
    return prices


def _load_position_usage_by_exposure(
    session: Session,
    *,
    business_date: date,
    exposure_ids: list[int],
) -> tuple[bool, dict[int, Any]]:
    has_rows = (
        session.scalar(
            select(func.count())
            .select_from(PortfolioPositionCurrent)
            .where(PortfolioPositionCurrent.business_date == business_date)
        )
        or 0
    ) > 0
    if not exposure_ids:
        return has_rows, {}
    rows = session.execute(
        select(
            PortfolioPositionCurrent.market_exposure_id,
            func.sum(PortfolioPositionCurrent.supply_usd).label("total_supply_usd"),
            func.sum(PortfolioPositionCurrent.borrow_usd).label("total_borrow_usd"),
            (
                func.sum(
                    (PortfolioPositionCurrent.supply_apy + PortfolioPositionCurrent.reward_apy)
                    * PortfolioPositionCurrent.supply_usd
                )
                / func.nullif(func.sum(PortfolioPositionCurrent.supply_usd), 0)
            ).label("collateral_yield_apy"),
        )
        .where(
            PortfolioPositionCurrent.business_date == business_date,
            PortfolioPositionCurrent.market_exposure_id.in_(exposure_ids),
        )
        .group_by(PortfolioPositionCurrent.market_exposure_id)
    ).all()
    return has_rows, {
        int(row.market_exposure_id): row for row in rows if row.market_exposure_id is not None
    }


def _load_borrow_usage_by_exposure(
    session: Session,
    *,
    exposure_ids: list[int],
) -> dict[int, Decimal]:
    if not exposure_ids:
        return {}
    slug_rows = session.execute(
        select(MarketExposure.market_exposure_id, MarketExposure.exposure_slug).where(
            MarketExposure.market_exposure_id.in_(exposure_ids)
        )
    ).all()
    metrics_by_slug = build_market_exposure_usage_metrics(session)
    output: dict[int, Decimal] = {}
    for market_exposure_id, exposure_slug in slug_rows:
        metrics = metrics_by_slug.get(str(exposure_slug))
        if metrics is None:
            continue
        _monitored, _strategy_position_count, borrow_usd = metrics
        output[int(market_exposure_id)] = borrow_usd
    return output


def _load_component_context(
    session: Session,
    *,
    business_date: date,
    exposure_ids: list[int],
) -> dict[int, list[Any]]:
    if not exposure_ids:
        return {}
    component_base_token = aliased(Token)
    component_collateral_token = aliased(Token)
    rows = session.execute(
        select(
            MarketExposureComponent.market_exposure_id,
            MarketExposureComponent.component_role,
            Market.market_id,
            Market.market_kind,
            Market.base_asset_token_id,
            Market.collateral_token_id,
            component_base_token.symbol.label("base_symbol"),
            component_collateral_token.symbol.label("collateral_symbol"),
            MarketOverviewDaily.max_ltv,
        )
        .join(Market, Market.market_id == MarketExposureComponent.market_id)
        .outerjoin(
            MarketOverviewDaily,
            (MarketOverviewDaily.market_id == Market.market_id)
            & (MarketOverviewDaily.business_date == business_date),
        )
        .outerjoin(
            component_base_token,
            component_base_token.token_id == Market.base_asset_token_id,
        )
        .outerjoin(
            component_collateral_token,
            component_collateral_token.token_id == Market.collateral_token_id,
        )
        .where(MarketExposureComponent.market_exposure_id.in_(exposure_ids))
    ).all()
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(int(row.market_exposure_id), []).append(row)
    return grouped


def _load_latest_caps_by_market(
    session: Session,
    *,
    market_ids: set[int],
) -> dict[int, dict[str, Any] | None]:
    if not market_ids:
        return {}
    rows = session.execute(
        select(MarketSnapshot.market_id, MarketSnapshot.caps_json)
        .where(MarketSnapshot.market_id.in_(market_ids))
        .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.as_of_ts_utc.desc())
    ).all()
    caps_by_market: dict[int, dict[str, Any] | None] = {}
    for market_id, caps_json in rows:
        caps_by_market.setdefault(
            int(market_id),
            caps_json if isinstance(caps_json, dict) else None,
        )
    return caps_by_market


def _build_exposure_context(
    session: Session,
    *,
    business_date: date,
    exposure_ids: list[int],
) -> dict[int, dict[str, Decimal | None]]:
    component_rows_by_exposure = _load_component_context(
        session, business_date=business_date, exposure_ids=exposure_ids
    )
    portfolio_has_rows, usage_by_exposure = _load_position_usage_by_exposure(
        session, business_date=business_date, exposure_ids=exposure_ids
    )
    borrow_usage_by_exposure = _load_borrow_usage_by_exposure(
        session,
        exposure_ids=exposure_ids,
    )
    caps_by_market = _load_latest_caps_by_market(
        session,
        market_ids={
            int(row.market_id) for rows in component_rows_by_exposure.values() for row in rows
        },
    )

    token_ids: set[int] = set()
    for rows in component_rows_by_exposure.values():
        supply_component = _pick_component(rows, "supply_market", "primary_market")
        borrow_component = _pick_component(rows, "borrow_market", "primary_market")
        supply_token_id = (
            _supply_component_token_id(supply_component) if supply_component is not None else None
        )
        borrow_token_id = (
            int(borrow_component.base_asset_token_id)
            if borrow_component is not None and borrow_component.base_asset_token_id is not None
            else None
        )
        if supply_token_id is not None:
            token_ids.add(int(supply_token_id))
        if borrow_token_id is not None:
            token_ids.add(borrow_token_id)
    prices = _load_latest_price_map(session, token_ids)

    output: dict[int, dict[str, Decimal | None]] = {}
    for exposure_id, rows in component_rows_by_exposure.items():
        supply_component = _pick_component(rows, "supply_market", "primary_market")
        borrow_component = _pick_component(rows, "borrow_market", "primary_market")
        supply_token_id = (
            _supply_component_token_id(supply_component) if supply_component is not None else None
        )
        borrow_token_id = (
            int(borrow_component.base_asset_token_id)
            if borrow_component is not None and borrow_component.base_asset_token_id is not None
            else None
        )
        supply_cap_usd = (
            _cap_usd(
                (
                    caps_by_market.get(int(supply_component.market_id))
                    if supply_component is not None
                    else None
                ),
                "supply_cap",
                prices.get(int(supply_token_id)) if supply_token_id is not None else None,
            )
            if supply_component is not None
            else None
        )
        borrow_cap_usd = (
            _cap_usd(
                (
                    caps_by_market.get(int(borrow_component.market_id))
                    if borrow_component is not None
                    else None
                ),
                "borrow_cap",
                prices.get(borrow_token_id) if borrow_token_id is not None else None,
            )
            if borrow_component is not None
            else None
        )
        collateral_max_ltv = None
        if supply_component is not None and supply_component.max_ltv is not None:
            max_ltv = Decimal(str(supply_component.max_ltv))
            if max_ltv > ZERO:
                collateral_max_ltv = max_ltv

        usage_row = usage_by_exposure.get(exposure_id)
        collateral_yield_apy = (
            Decimal(str(usage_row.collateral_yield_apy))
            if usage_row is not None and usage_row.collateral_yield_apy is not None
            else None
        )
        avant_borrow_usd = borrow_usage_by_exposure.get(exposure_id, ZERO)
        output[exposure_id] = {
            "supply_cap_usd": supply_cap_usd,
            "borrow_cap_usd": borrow_cap_usd,
            "collateral_max_ltv": collateral_max_ltv,
            "collateral_yield_apy": collateral_yield_apy,
            "avant_borrow_usd": avant_borrow_usd if portfolio_has_rows else None,
        }
    return output


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


def _build_exposure_row(
    row: Any,
    *,
    context: dict[str, Decimal | None] | None = None,
) -> MarketExposureRow:
    context = context or {}
    collateral_yield_apy = context.get("collateral_yield_apy")
    if collateral_yield_apy is None:
        collateral_yield_apy = row.weighted_supply_apy
    avant_borrow_share = None
    avant_borrow_usd = context.get("avant_borrow_usd")
    if avant_borrow_usd is not None and row.total_borrow_usd > ZERO:
        avant_borrow_share = avant_borrow_usd / row.total_borrow_usd
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
        collateral_yield_apy=collateral_yield_apy,
        weighted_borrow_apy=row.weighted_borrow_apy,
        spread_apy=collateral_yield_apy - row.weighted_borrow_apy,
        utilization=row.utilization,
        available_liquidity_usd=row.available_liquidity_usd,
        supply_cap_usd=context.get("supply_cap_usd"),
        borrow_cap_usd=context.get("borrow_cap_usd"),
        collateral_max_ltv=context.get("collateral_max_ltv"),
        avant_borrow_share=avant_borrow_share,
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
    rows = session.execute(stmt).all()
    context_by_exposure = _build_exposure_context(
        session,
        business_date=business_date,
        exposure_ids=[int(row.market_exposure_id) for row in rows],
    )
    return [
        _build_exposure_row(
            row,
            context=context_by_exposure.get(int(row.market_exposure_id)),
        )
        for row in rows
    ]


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

    context_by_exposure = _build_exposure_context(
        session,
        business_date=business_date,
        exposure_ids=[int(exposure_row.market_exposure_id)],
    )
    exposure = _build_exposure_row(
        exposure_row,
        context=context_by_exposure.get(int(exposure_row.market_exposure_id)),
    )
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
            MarketExposureComponent.component_role,
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
            component_role=row.component_role,
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
            literal(None).label("component_role"),
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
        component_role=component_row.component_role,
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
