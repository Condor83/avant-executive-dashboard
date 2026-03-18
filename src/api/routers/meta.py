"""UI metadata endpoint for server-owned labels, filters, and sort options."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.common import OptionItem
from api.schemas.meta import BenchmarkYield, UiMetadataResponse
from core.dashboard_contracts import (
    ALERT_SEVERITY_LABELS,
    ALERT_STATUS_LABELS,
    POSITION_SORT_OPTIONS,
    PRODUCT_BENCHMARK_TOKEN_MAP,
    product_label,
)
from core.db.models import Chain, PortfolioPositionCurrent, Product, Protocol, Wallet
from core.settings import get_settings
from core.yields import AvantYieldOracle

router = APIRouter(prefix="/meta")
logger = logging.getLogger(__name__)

_PRODUCT_BENCHMARK_MAP = PRODUCT_BENCHMARK_TOKEN_MAP


def _fetch_benchmarks() -> list[BenchmarkYield]:
    """Fetch live senior token APYs from the Avant API."""
    settings = get_settings()
    oracle = AvantYieldOracle(
        base_url=settings.avant_api_base_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    try:
        apy_by_symbol: dict[str, str] = {}
        benchmarks: list[BenchmarkYield] = []
        for product_code, symbol in _PRODUCT_BENCHMARK_MAP.items():
            apy = apy_by_symbol.get(symbol)
            if apy is None:
                try:
                    apy = str(oracle.get_token_apy(symbol))
                    apy_by_symbol[symbol] = apy
                except Exception:
                    logger.warning("failed to fetch benchmark APY for %s", symbol, exc_info=True)
                    continue
            benchmarks.append(
                BenchmarkYield(
                    product_code=product_code,
                    token_symbol=symbol.lower(),
                    apy=apy,
                )
            )
        return benchmarks
    finally:
        oracle.close()


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

    benchmarks = _fetch_benchmarks()

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
        benchmarks=benchmarks,
    )
