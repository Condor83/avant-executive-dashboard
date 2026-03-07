"""UI metadata endpoint for server-owned labels, filters, and sort options."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.common import OptionItem
from api.schemas.meta import UiMetadataResponse
from core.dashboard_contracts import (
    ALERT_SEVERITY_LABELS,
    ALERT_STATUS_LABELS,
    POSITION_SORT_OPTIONS,
    product_label,
)
from core.db.models import Chain, PortfolioPositionCurrent, Product, Protocol, Wallet

router = APIRouter(prefix="/meta")


def _labelize_code(value: str) -> str:
    return value.replace("_", " ").title()


def _wallet_label(address: str, product_code: str | None, label: str | None) -> str:
    wallet_label = label or f"{address[:6]}...{address[-4:]}"
    product = product_label(product_code)
    if product:
        return f"{product.split(' (')[0]} · {wallet_label}"
    return wallet_label


@router.get("/ui")
def get_ui_metadata(session: Session = Depends(get_session)) -> UiMetadataResponse:
    products = session.scalars(select(Product).order_by(Product.product_code.asc())).all()
    protocols = session.scalars(select(Protocol).order_by(Protocol.protocol_code.asc())).all()
    chains = session.scalars(select(Chain).order_by(Chain.chain_code.asc())).all()
    wallets = session.execute(
        select(
            Wallet.address,
            Wallet.label,
            Product.product_code,
        )
        .join(PortfolioPositionCurrent, PortfolioPositionCurrent.wallet_id == Wallet.wallet_id)
        .outerjoin(Product, Product.product_id == PortfolioPositionCurrent.product_id)
        .distinct()
        .order_by(Product.product_code.asc(), Wallet.address.asc())
    ).all()

    return UiMetadataResponse(
        products=[
            OptionItem(
                value=product.product_code,
                label=product_label(product.product_code) or product.product_code,
            )
            for product in products
        ],
        protocols=[
            OptionItem(value=protocol.protocol_code, label=_labelize_code(protocol.protocol_code))
            for protocol in protocols
        ],
        chains=[
            OptionItem(value=chain.chain_code, label=_labelize_code(chain.chain_code))
            for chain in chains
        ],
        wallets=[
            OptionItem(
                value=address,
                label=_wallet_label(address=address, product_code=product_code, label=label),
            )
            for address, label, product_code in wallets
        ],
        position_sort_options=[
            OptionItem(value=value, label=label) for value, label in POSITION_SORT_OPTIONS
        ],
        alert_severity_options=[
            OptionItem(value=value, label=label) for value, label in ALERT_SEVERITY_LABELS.items()
        ],
        alert_status_options=[
            OptionItem(value=value, label=label) for value, label in ALERT_STATUS_LABELS.items()
        ],
    )
