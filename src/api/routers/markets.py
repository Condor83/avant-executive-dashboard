"""Markets endpoints: overview, history, and watchlist."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.alerts import AlertRow
from api.schemas.markets import MarketHistoryPoint, MarketOverviewRow, WatchlistRow
from core.db.models import Alert, Chain, Market, MarketOverviewDaily, Protocol, Token

router = APIRouter(prefix="/markets")


def _build_overview_rows(session: Session, business_date: date) -> list[MarketOverviewRow]:
    mod = MarketOverviewDaily
    rows = session.execute(
        select(
            mod.market_id,
            Protocol.protocol_code,
            Chain.chain_code,
            Market.market_address,
            Token.symbol.label("base_asset_symbol"),
            mod.total_supply_usd,
            mod.total_borrow_usd,
            mod.utilization,
            mod.supply_apy,
            mod.borrow_apy,
            mod.spread_apy,
            mod.available_liquidity_usd,
            mod.avant_supplied_usd,
            mod.avant_borrowed_usd,
            mod.avant_supply_share,
            mod.avant_borrow_share,
            mod.max_ltv,
            mod.liquidation_threshold,
            mod.liquidation_penalty,
        )
        .join(Market, Market.market_id == mod.market_id)
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(Token, Token.token_id == Market.base_asset_token_id)
        .where(mod.business_date == business_date)
        .order_by(mod.total_supply_usd.desc())
    ).all()

    # Count open alerts per market
    alert_counts: dict[int, int] = {}
    alert_rows = session.execute(
        select(Alert.entity_id, func.count())
        .where(Alert.status == "open", Alert.entity_type == "market")
        .group_by(Alert.entity_id)
    ).all()
    for entity_id, cnt in alert_rows:
        try:
            alert_counts[int(entity_id)] = cnt
        except (ValueError, TypeError):
            pass

    result = []
    for r in rows:
        result.append(
            MarketOverviewRow(
                market_id=r[0],
                protocol_code=r[1],
                chain_code=r[2],
                market_address=r[3],
                base_asset_symbol=r[4],
                total_supply_usd=r[5],
                total_borrow_usd=r[6],
                utilization=r[7],
                supply_apy=r[8],
                borrow_apy=r[9],
                spread_apy=r[10],
                available_liquidity_usd=r[11],
                avant_supplied_usd=r[12],
                avant_borrowed_usd=r[13],
                avant_supply_share=r[14],
                avant_borrow_share=r[15],
                max_ltv=r[16],
                liquidation_threshold=r[17],
                liquidation_penalty=r[18],
                open_alert_count=alert_counts.get(r[0], 0),
            )
        )
    return result


@router.get("/overview")
def get_overview(
    business_date: date | None = Query(default=None),
    session: Session = Depends(get_session),
) -> list[MarketOverviewRow]:
    if business_date is None:
        business_date = session.scalar(select(func.max(MarketOverviewDaily.business_date)))
    if business_date is None:
        return []
    return _build_overview_rows(session, business_date)


@router.get("/watchlist")
def get_watchlist(
    session: Session = Depends(get_session),
) -> list[WatchlistRow]:
    latest_date = session.scalar(select(func.max(MarketOverviewDaily.business_date)))
    if latest_date is None:
        return []

    overview_rows = _build_overview_rows(session, latest_date)

    # Load open alerts grouped by market entity_id
    open_alerts = session.scalars(
        select(Alert).where(Alert.status == "open", Alert.entity_type == "market")
    ).all()
    alerts_by_market: dict[int, list[AlertRow]] = {}
    for a in open_alerts:
        try:
            mid = int(a.entity_id)
        except (ValueError, TypeError):
            continue
        alerts_by_market.setdefault(mid, []).append(
            AlertRow(
                alert_id=a.alert_id,
                ts_utc=a.ts_utc,
                alert_type=a.alert_type,
                severity=a.severity,
                entity_type=a.entity_type,
                entity_id=a.entity_id,
                payload_json=a.payload_json,
                status=a.status,
            )
        )

    result = []
    for row in overview_rows:
        market_alerts = alerts_by_market.get(row.market_id, [])
        if market_alerts:
            result.append(
                WatchlistRow(
                    **row.model_dump(),
                    alerts=market_alerts,
                )
            )
    return result


@router.get("/{market_id}/history")
def get_market_history(
    market_id: int,
    days: int = Query(default=30, ge=1, le=365),
    session: Session = Depends(get_session),
) -> list[MarketHistoryPoint]:
    market = session.get(Market, market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="market not found")

    latest_date = session.scalar(
        select(func.max(MarketOverviewDaily.business_date)).where(
            MarketOverviewDaily.market_id == market_id
        )
    )
    if latest_date is None:
        return []

    start_date = latest_date - timedelta(days=days - 1)
    rows = session.execute(
        select(
            MarketOverviewDaily.business_date,
            MarketOverviewDaily.total_supply_usd,
            MarketOverviewDaily.total_borrow_usd,
            MarketOverviewDaily.utilization,
            MarketOverviewDaily.supply_apy,
            MarketOverviewDaily.borrow_apy,
            MarketOverviewDaily.spread_apy,
            MarketOverviewDaily.avant_supplied_usd,
            MarketOverviewDaily.avant_borrowed_usd,
        )
        .where(
            MarketOverviewDaily.market_id == market_id,
            MarketOverviewDaily.business_date >= start_date,
            MarketOverviewDaily.business_date <= latest_date,
        )
        .order_by(MarketOverviewDaily.business_date.asc())
    ).all()

    return [
        MarketHistoryPoint(
            business_date=r[0],
            total_supply_usd=r[1],
            total_borrow_usd=r[2],
            utilization=r[3],
            supply_apy=r[4],
            borrow_apy=r[5],
            spread_apy=r[6],
            avant_supplied_usd=r[7],
            avant_borrowed_usd=r[8],
        )
        for r in rows
    ]
