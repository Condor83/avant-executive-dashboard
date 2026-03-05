"""Portfolio endpoints: products, wallets, and positions."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from analytics.rollups import RollupMetrics, compute_window_rollups
from analytics.yield_engine import METHOD_APY_PRORATED_SOD_EOD
from api.deps import get_session
from api.schemas.common import YieldMetrics
from api.schemas.portfolio import PaginatedPositions, PositionRow, ProductRow
from core.db.models import (
    Chain,
    Market,
    PositionSnapshot,
    Product,
    Protocol,
    Wallet,
    WalletProductMap,
    YieldDaily,
)

router = APIRouter(prefix="/portfolio")

ZERO = Decimal("0")

POSITION_SORT_COLUMNS = {
    "equity_usd": PositionSnapshot.equity_usd,
    "supplied_usd": PositionSnapshot.supplied_usd,
    "borrowed_usd": PositionSnapshot.borrowed_usd,
    "gross_yield_usd": YieldDaily.gross_yield_usd,
    "net_yield_usd": YieldDaily.net_yield_usd,
}


def _rollup_to_yield(m: RollupMetrics) -> YieldMetrics:
    return YieldMetrics(
        gross_yield_usd=m.gross_yield_usd,
        strategy_fee_usd=m.strategy_fee_usd,
        avant_gop_usd=m.avant_gop_usd,
        net_yield_usd=m.net_yield_usd,
        avg_equity_usd=m.avg_equity_usd,
        gross_roe=m.gross_roe,
        net_roe=m.net_roe,
    )


def _yield_daily_to_metrics(row: YieldDaily) -> YieldMetrics:
    return YieldMetrics(
        gross_yield_usd=row.gross_yield_usd,
        strategy_fee_usd=row.strategy_fee_usd,
        avant_gop_usd=row.avant_gop_usd,
        net_yield_usd=row.net_yield_usd,
        avg_equity_usd=row.avg_equity_usd or ZERO,
        gross_roe=row.gross_roe,
        net_roe=row.net_roe,
    )


def _zero_yield() -> YieldMetrics:
    return YieldMetrics(
        gross_yield_usd=ZERO,
        strategy_fee_usd=ZERO,
        avant_gop_usd=ZERO,
        net_yield_usd=ZERO,
        avg_equity_usd=ZERO,
        gross_roe=None,
        net_roe=None,
    )


@router.get("/products")
def get_products(session: Session = Depends(get_session)) -> list[ProductRow]:
    latest_date = session.scalar(
        select(func.max(YieldDaily.business_date)).where(
            YieldDaily.position_key.is_(None),
            YieldDaily.product_id.is_not(None),
            YieldDaily.wallet_id.is_(None),
            YieldDaily.protocol_id.is_(None),
        )
    )

    products = session.scalars(select(Product).order_by(Product.product_id)).all()
    if not products:
        return []

    # Yesterday: product rollup rows from yield_daily
    yesterday_map: dict[int, YieldMetrics] = {}
    if latest_date is not None:
        product_rows = session.scalars(
            select(YieldDaily).where(
                YieldDaily.business_date == latest_date,
                YieldDaily.position_key.is_(None),
                YieldDaily.product_id.is_not(None),
                YieldDaily.wallet_id.is_(None),
                YieldDaily.protocol_id.is_(None),
            )
        ).all()
        for r in product_rows:
            if r.product_id is not None:
                yesterday_map[r.product_id] = _yield_daily_to_metrics(r)

    # Trailing windows
    rollup_7d = compute_window_rollups(session, window="7d")
    rollup_30d = compute_window_rollups(session, window="30d")
    map_7d = {gr.entity_id: _rollup_to_yield(gr.metrics) for gr in rollup_7d.product_rollups}
    map_30d = {gr.entity_id: _rollup_to_yield(gr.metrics) for gr in rollup_30d.product_rollups}

    result = []
    for p in products:
        result.append(
            ProductRow(
                product_id=p.product_id,
                product_code=p.product_code,
                yesterday=yesterday_map.get(p.product_id, _zero_yield()),
                trailing_7d=map_7d.get(p.product_id, _zero_yield()),
                trailing_30d=map_30d.get(p.product_id, _zero_yield()),
            )
        )
    return result


@router.get("/wallets/{address}")
def get_wallet(address: str, session: Session = Depends(get_session)) -> dict:
    wallet = session.scalar(select(Wallet).where(Wallet.address == address))
    if wallet is None:
        raise HTTPException(status_code=404, detail="wallet not found")

    latest_date = session.scalar(
        select(func.max(YieldDaily.business_date)).where(
            YieldDaily.position_key.is_(None),
            YieldDaily.wallet_id == wallet.wallet_id,
            YieldDaily.product_id.is_(None),
            YieldDaily.protocol_id.is_(None),
        )
    )

    if latest_date is not None:
        yd_row = session.scalar(
            select(YieldDaily).where(
                YieldDaily.business_date == latest_date,
                YieldDaily.wallet_id == wallet.wallet_id,
                YieldDaily.position_key.is_(None),
                YieldDaily.product_id.is_(None),
                YieldDaily.protocol_id.is_(None),
            )
        )
        yesterday = _yield_daily_to_metrics(yd_row) if yd_row else _zero_yield()
    else:
        yesterday = _zero_yield()

    rollup_7d = compute_window_rollups(session, window="7d")
    rollup_30d = compute_window_rollups(session, window="30d")
    wid = wallet.wallet_id
    w7d = next(
        (_rollup_to_yield(gr.metrics) for gr in rollup_7d.wallet_rollups if gr.entity_id == wid),
        _zero_yield(),
    )
    w30d = next(
        (_rollup_to_yield(gr.metrics) for gr in rollup_30d.wallet_rollups if gr.entity_id == wid),
        _zero_yield(),
    )

    return {
        "wallet_id": wallet.wallet_id,
        "address": wallet.address,
        "wallet_type": wallet.wallet_type,
        "yesterday": yesterday.model_dump(),
        "trailing_7d": w7d.model_dump(),
        "trailing_30d": w30d.model_dump(),
    }


@router.get("/positions")
def get_positions(
    product_code: str | None = Query(default=None),
    protocol_code: str | None = Query(default=None),
    chain_code: str | None = Query(default=None),
    wallet_address: str | None = Query(default=None),
    business_date: date | None = Query(default=None),
    sort_by: str = Query(default="equity_usd"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_session),
) -> PaginatedPositions:
    # Resolve effective business date
    if business_date is None:
        business_date = session.scalar(select(func.max(YieldDaily.business_date)))
    if business_date is None:
        return PaginatedPositions(
            as_of_date=date.today(),
            total_count=0,
            page=page,
            page_size=page_size,
            positions=[],
        )

    # Find latest position snapshot timestamp
    latest_ps_ts = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
    if latest_ps_ts is None:
        return PaginatedPositions(
            as_of_date=business_date,
            total_count=0,
            page=page,
            page_size=page_size,
            positions=[],
        )

    # Build base query joining position_snapshots with yield_daily and dimension tables
    ps = PositionSnapshot
    yd = YieldDaily

    base_stmt = (
        select(
            ps.position_key,
            Wallet.address.label("wallet_address"),
            Product.product_code,
            Protocol.protocol_code,
            Chain.chain_code,
            Market.market_address,
            ps.supplied_usd,
            ps.borrowed_usd,
            ps.equity_usd,
            ps.supply_apy,
            ps.borrow_apy,
            ps.reward_apy,
            ps.health_factor,
            ps.ltv,
            yd.gross_yield_usd,
            yd.net_yield_usd,
            yd.gross_roe,
        )
        .join(Wallet, Wallet.wallet_id == ps.wallet_id)
        .join(Market, Market.market_id == ps.market_id)
        .join(Protocol, Protocol.protocol_id == Market.protocol_id)
        .join(Chain, Chain.chain_id == Market.chain_id)
        .outerjoin(WalletProductMap, WalletProductMap.wallet_id == ps.wallet_id)
        .outerjoin(Product, Product.product_id == WalletProductMap.product_id)
        .outerjoin(
            yd,
            (yd.position_key == ps.position_key)
            & (yd.business_date == business_date)
            & (yd.method == METHOD_APY_PRORATED_SOD_EOD),
        )
        .where(ps.as_of_ts_utc == latest_ps_ts)
    )

    # Apply filters
    if product_code is not None:
        base_stmt = base_stmt.where(Product.product_code == product_code)
    if protocol_code is not None:
        base_stmt = base_stmt.where(Protocol.protocol_code == protocol_code)
    if chain_code is not None:
        base_stmt = base_stmt.where(Chain.chain_code == chain_code)
    if wallet_address is not None:
        base_stmt = base_stmt.where(Wallet.address == wallet_address)

    # Count

    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    total_count = session.scalar(count_stmt) or 0

    # Sort
    sort_col = POSITION_SORT_COLUMNS.get(sort_by, ps.equity_usd)
    if sort_dir == "asc":
        base_stmt = base_stmt.order_by(sort_col.asc())
    else:
        base_stmt = base_stmt.order_by(sort_col.desc())

    # Paginate
    offset = (page - 1) * page_size
    base_stmt = base_stmt.offset(offset).limit(page_size)

    rows = session.execute(base_stmt).all()
    positions = [
        PositionRow(
            position_key=r[0],
            wallet_address=r[1],
            product_code=r[2],
            protocol_code=r[3],
            chain_code=r[4],
            market_address=r[5],
            supplied_usd=r[6],
            borrowed_usd=r[7],
            equity_usd=r[8],
            supply_apy=r[9],
            borrow_apy=r[10],
            reward_apy=r[11],
            health_factor=r[12],
            ltv=r[13],
            gross_yield_usd=r[14],
            net_yield_usd=r[15],
            gross_roe=r[16],
        )
        for r in rows
    ]

    return PaginatedPositions(
        as_of_date=business_date,
        total_count=total_count,
        page=page,
        page_size=page_size,
        positions=positions,
    )
