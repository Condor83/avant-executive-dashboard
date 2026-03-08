"""Served wallet summary endpoints built from current portfolio rows."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.wallets import WalletsResponse, WalletSummaryRow
from core.dashboard_contracts import product_label
from core.db.models import PortfolioPositionCurrent, Product, Wallet

router = APIRouter(prefix="/wallets")


def _wallet_label(address: str, label: str | None) -> str:
    return label or f"{address[:6]}...{address[-4:]}"


@router.get("/current")
def get_wallets_current(session: Session = Depends(get_session)) -> WalletsResponse:
    business_date = session.scalar(select(func.max(PortfolioPositionCurrent.business_date)))
    if business_date is None:
        return WalletsResponse(
            business_date=datetime.now(UTC).date(),
            total_count=0,
            wallets=[],
        )

    rows = session.execute(
        select(
            Wallet.address.label("wallet_address"),
            Wallet.label.label("wallet_name"),
            Product.product_code,
            func.sum(PortfolioPositionCurrent.supply_usd).label("total_supply_usd"),
            func.sum(PortfolioPositionCurrent.borrow_usd).label("total_borrow_usd"),
            func.sum(PortfolioPositionCurrent.net_equity_usd).label("total_tvl_usd"),
        )
        .join(Wallet, Wallet.wallet_id == PortfolioPositionCurrent.wallet_id)
        .outerjoin(Product, Product.product_id == PortfolioPositionCurrent.product_id)
        .where(PortfolioPositionCurrent.business_date == business_date)
        .where(PortfolioPositionCurrent.scope_segment == "strategy_only")
        .where(Wallet.wallet_type == "strategy")
        .group_by(Wallet.address, Wallet.label, Product.product_code)
        .order_by(func.sum(PortfolioPositionCurrent.net_equity_usd).desc(), Wallet.address.asc())
    ).all()

    wallet_rows = [
        WalletSummaryRow(
            wallet_address=row.wallet_address,
            wallet_label=_wallet_label(row.wallet_address, row.wallet_name),
            product_code=row.product_code,
            product_label=product_label(row.product_code),
            total_supply_usd=row.total_supply_usd,
            total_borrow_usd=row.total_borrow_usd,
            total_tvl_usd=row.total_tvl_usd,
        )
        for row in rows
    ]

    return WalletsResponse(
        business_date=business_date,
        total_count=len(wallet_rows),
        wallets=wallet_rows,
    )
