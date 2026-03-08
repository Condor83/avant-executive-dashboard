"""Served portfolio endpoints built from canonical position views."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from api.deps import get_session
from api.schemas.common import RoeMetrics, YieldWindow
from api.schemas.portfolio import (
    PortfolioPositionDetailResponse,
    PortfolioPositionHistoryPoint,
    PortfolioPositionRow,
    PortfolioPositionsResponse,
    PortfolioSummaryResponse,
    PositionLeg,
)
from core.dashboard_contracts import code_label, leverage_ratio, product_label
from core.db.models import (
    Chain,
    Market,
    MarketExposure,
    PortfolioPositionCurrent,
    PortfolioPositionDaily,
    PortfolioSummaryDaily,
    Position,
    Product,
    Protocol,
    Token,
    Wallet,
    YieldDaily,
)

router = APIRouter(prefix="/portfolio")

ZERO = Decimal("0")
METHOD = "apy_prorated_sod_eod"
ANNUALIZATION_DAYS = Decimal("365")
ROE_QUANTUM = Decimal("0.0000000001")
DUST_BORROW_USD = Decimal("1")
SUPPLY_TOKEN = aliased(Token)
BORROW_TOKEN = aliased(Token)


@dataclass(frozen=True)
class _PositionAggregate:
    position_id: int
    position_key: str
    display_name: str
    wallet_address: str
    product_code: str | None
    protocol_code: str
    chain_code: str
    position_kind: str
    market_exposure_slug: str | None
    supply_legs: tuple[PositionLeg, ...]
    borrow_legs: tuple[PositionLeg, ...]
    net_equity_usd: Decimal
    leverage_ratio: Decimal | None
    health_factor: Decimal | None
    gross_yield_daily_usd: Decimal
    net_yield_daily_usd: Decimal
    gross_yield_mtd_usd: Decimal
    net_yield_mtd_usd: Decimal
    strategy_fee_daily_usd: Decimal
    avant_gop_daily_usd: Decimal
    strategy_fee_mtd_usd: Decimal
    avant_gop_mtd_usd: Decimal
    gross_roe: Decimal | None
    net_roe: Decimal | None
    member_position_keys: tuple[str, ...]


def _wallet_label(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}"


def _yield_window(
    *,
    gross_yield_usd: Decimal,
    strategy_fee_usd: Decimal,
    avant_gop_usd: Decimal,
    net_yield_usd: Decimal,
) -> YieldWindow:
    return YieldWindow(
        gross_yield_usd=gross_yield_usd,
        strategy_fee_usd=strategy_fee_usd,
        avant_gop_usd=avant_gop_usd,
        net_yield_usd=net_yield_usd,
    )


def _normalized_roe(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(ROE_QUANTUM)


def _annualize_daily_roe(value: Decimal | None) -> Decimal | None:
    normalized = _normalized_roe(value)
    if normalized is None:
        return None
    return normalized * ANNUALIZATION_DAYS


def _roe_metrics(*, gross_roe: Decimal | None, net_roe: Decimal | None) -> RoeMetrics:
    gross_roe_daily = _normalized_roe(gross_roe)
    net_roe_daily = _normalized_roe(net_roe)
    return RoeMetrics(
        gross_roe_daily=gross_roe_daily,
        gross_roe_annualized=_annualize_daily_roe(gross_roe_daily),
        net_roe_daily=net_roe_daily,
        net_roe_annualized=_annualize_daily_roe(net_roe_daily),
    )


def _build_supply_leg(row: Any) -> PositionLeg:
    return PositionLeg(
        token_id=row.supply_token_id,
        symbol=row.supply_symbol,
        amount=row.supply_amount,
        usd_value=row.supply_usd,
        apy=(row.supply_apy or ZERO) + (row.reward_apy or ZERO),
        estimated_daily_cashflow_usd=row.gross_yield_daily_usd,
    )


def _build_borrow_leg(row: Any) -> PositionLeg | None:
    if row.borrow_token_id is None:
        return None
    return PositionLeg(
        token_id=row.borrow_token_id,
        symbol=row.borrow_symbol,
        amount=row.borrow_amount,
        usd_value=row.borrow_usd,
        apy=row.borrow_apy or ZERO,
        estimated_daily_cashflow_usd=-(row.borrow_usd * (row.borrow_apy or ZERO) / Decimal("365")),
    )


def _display_name(
    *,
    supply_legs: tuple[PositionLeg, ...],
    borrow_legs: tuple[PositionLeg, ...],
    protocol_code: str,
    chain_code: str,
) -> str:
    supply_symbols = [leg.symbol or "Unknown" for leg in supply_legs if leg.usd_value > ZERO]
    if not supply_symbols and supply_legs:
        supply_symbols = [supply_legs[0].symbol or "Unknown"]
    supply_label = " + ".join(supply_symbols) if supply_symbols else "Unknown"
    borrow_symbols = [leg.symbol or "Unknown" for leg in borrow_legs]
    borrow_label = " + ".join(borrow_symbols) if borrow_symbols else None
    protocol_chain = f"{code_label(protocol_code)}-{code_label(chain_code)}"
    if borrow_label:
        return f"{supply_label}/{borrow_label} {protocol_chain}"
    return f"{supply_label} {protocol_chain}"


def _total_supply_usd(legs: Sequence[PositionLeg]) -> Decimal:
    return sum((leg.usd_value for leg in legs), ZERO)


def _primary_supply_leg(legs: Sequence[PositionLeg]) -> PositionLeg:
    if not legs:
        raise ValueError("supply legs must not be empty")
    return max(legs, key=lambda leg: (leg.usd_value, leg.symbol or ""))


def _build_position_row(position: _PositionAggregate) -> PortfolioPositionRow:
    supply_legs = list(position.supply_legs)
    borrow_legs = list(position.borrow_legs)
    return PortfolioPositionRow(
        position_id=position.position_id,
        position_key=position.position_key,
        display_name=position.display_name,
        wallet_address=position.wallet_address,
        wallet_label=_wallet_label(position.wallet_address),
        product_code=position.product_code,
        product_label=product_label(position.product_code),
        protocol_code=position.protocol_code,
        chain_code=position.chain_code,
        position_kind=position.position_kind,
        market_exposure_slug=position.market_exposure_slug,
        supply_leg=_primary_supply_leg(supply_legs),
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        borrow_leg=borrow_legs[0] if len(borrow_legs) == 1 else None,
        net_equity_usd=position.net_equity_usd,
        leverage_ratio=position.leverage_ratio,
        health_factor=position.health_factor,
        roe=_roe_metrics(gross_roe=position.gross_roe, net_roe=position.net_roe),
        yield_daily=_yield_window(
            gross_yield_usd=position.gross_yield_daily_usd,
            strategy_fee_usd=position.strategy_fee_daily_usd,
            avant_gop_usd=position.avant_gop_daily_usd,
            net_yield_usd=position.net_yield_daily_usd,
        ),
        yield_mtd=_yield_window(
            gross_yield_usd=position.gross_yield_mtd_usd,
            strategy_fee_usd=position.strategy_fee_mtd_usd,
            avant_gop_usd=position.avant_gop_mtd_usd,
            net_yield_usd=position.net_yield_mtd_usd,
        ),
    )


def _positions_statement():
    return (
        select(
            PortfolioPositionCurrent.position_id,
            Position.position_key,
            Position.display_name,
            Wallet.address.label("wallet_address"),
            Product.product_code,
            Protocol.protocol_code,
            Chain.chain_code,
            Market.market_kind,
            MarketExposure.exposure_slug.label("market_exposure_slug"),
            PortfolioPositionCurrent.supply_token_id,
            SUPPLY_TOKEN.symbol.label("supply_symbol"),
            PortfolioPositionCurrent.supply_amount,
            PortfolioPositionCurrent.supply_usd,
            PortfolioPositionCurrent.supply_apy,
            PortfolioPositionCurrent.reward_apy,
            PortfolioPositionCurrent.borrow_token_id,
            BORROW_TOKEN.symbol.label("borrow_symbol"),
            PortfolioPositionCurrent.borrow_amount,
            PortfolioPositionCurrent.borrow_usd,
            PortfolioPositionCurrent.borrow_apy,
            PortfolioPositionCurrent.net_equity_usd,
            PortfolioPositionCurrent.leverage_ratio,
            PortfolioPositionCurrent.health_factor,
            PortfolioPositionCurrent.gross_yield_daily_usd,
            PortfolioPositionCurrent.net_yield_daily_usd,
            PortfolioPositionCurrent.gross_yield_mtd_usd,
            PortfolioPositionCurrent.net_yield_mtd_usd,
            PortfolioPositionCurrent.strategy_fee_daily_usd,
            PortfolioPositionCurrent.avant_gop_daily_usd,
            PortfolioPositionCurrent.strategy_fee_mtd_usd,
            PortfolioPositionCurrent.avant_gop_mtd_usd,
            PortfolioPositionCurrent.gross_roe,
            PortfolioPositionCurrent.net_roe,
        )
        .join(Position, Position.position_id == PortfolioPositionCurrent.position_id)
        .join(Wallet, Wallet.wallet_id == PortfolioPositionCurrent.wallet_id)
        .join(Protocol, Protocol.protocol_id == PortfolioPositionCurrent.protocol_id)
        .join(Chain, Chain.chain_id == PortfolioPositionCurrent.chain_id)
        .outerjoin(Market, Market.market_id == Position.market_id)
        .outerjoin(Product, Product.product_id == PortfolioPositionCurrent.product_id)
        .outerjoin(
            MarketExposure,
            MarketExposure.market_exposure_id == PortfolioPositionCurrent.market_exposure_id,
        )
        .join(SUPPLY_TOKEN, SUPPLY_TOKEN.token_id == PortfolioPositionCurrent.supply_token_id)
        .outerjoin(BORROW_TOKEN, BORROW_TOKEN.token_id == PortfolioPositionCurrent.borrow_token_id)
    )


def _latest_business_date(session: Session) -> date | None:
    return session.scalar(select(func.max(PortfolioPositionCurrent.business_date)))


def _avg_equity_map(
    session: Session,
    *,
    business_date: date,
    position_keys: list[str],
) -> dict[str, Decimal]:
    if not position_keys:
        return {}
    rows = session.execute(
        select(YieldDaily.position_key, YieldDaily.avg_equity_usd).where(
            YieldDaily.business_date == business_date,
            YieldDaily.method == METHOD,
            YieldDaily.position_key.in_(position_keys),
        )
    ).all()
    return {position_key: avg_equity_usd for position_key, avg_equity_usd in rows if position_key}


def _reserve_bucket_key(row: Any) -> tuple[str, str | None, str, str]:
    return (row.wallet_address, row.product_code, row.protocol_code, row.chain_code)


def _synthetic_position_key(row: Any) -> str:
    product = row.product_code or "unassigned"
    return f"paired-reserve:{row.protocol_code}:{row.chain_code}:{row.wallet_address}:{product}"


def _dolomite_account_number(position_key: str) -> str | None:
    parts = position_key.split(":", 4)
    if len(parts) != 5 or parts[0] != "dolomite":
        return None
    return parts[3]


def _dolomite_bucket_key(row: Any) -> tuple[str, str | None, str, str, str] | None:
    account_number = _dolomite_account_number(row.position_key)
    if account_number is None:
        return None
    return (
        row.wallet_address,
        row.product_code,
        row.protocol_code,
        row.chain_code,
        account_number,
    )


def _synthetic_dolomite_position_key(row: Any, *, account_number: str) -> str:
    return (
        "paired-dolomite:"
        f"{row.protocol_code}:{row.chain_code}:{row.wallet_address}:{account_number}"
    )


def _zest_bucket_key(row: Any) -> tuple[str, str | None, str, str] | None:
    if row.protocol_code != "zest":
        return None
    return (row.wallet_address, row.product_code, row.protocol_code, row.chain_code)


def _synthetic_zest_position_key(row: Any) -> str:
    product = row.product_code or "unassigned"
    return f"paired-zest:{row.protocol_code}:{row.chain_code}:{row.wallet_address}:{product}"


def _stakedao_vault_address(position_key: str) -> str | None:
    parts = position_key.split(":", 4)
    if len(parts) != 5 or parts[0] != "stakedao":
        return None
    return parts[3]


def _stakedao_bucket_key(row: Any) -> tuple[str, str | None, str, str, str] | None:
    vault_address = _stakedao_vault_address(row.position_key)
    if vault_address is None:
        return None
    return (
        row.wallet_address,
        row.product_code,
        row.protocol_code,
        row.chain_code,
        vault_address,
    )


def _synthetic_stakedao_position_key(row: Any, *, vault_address: str) -> str:
    return (
        f"curated-vault:{row.protocol_code}:{row.chain_code}:{row.wallet_address}:{vault_address}"
    )


def _is_split_position_row(row: Any) -> bool:
    return (row.supply_usd > ZERO and row.borrow_usd <= ZERO) or (
        row.supply_usd <= ZERO and row.borrow_usd > ZERO
    )


def _is_pairable_reserve_bucket(rows: list[Any]) -> bool:
    if len(rows) <= 1 or any(row.market_kind != "reserve" for row in rows):
        return False
    if any(not _is_split_position_row(row) for row in rows):
        return False
    supply_rows = [row for row in rows if row.supply_usd > ZERO]
    borrow_rows = [row for row in rows if row.borrow_usd > ZERO]
    return bool(supply_rows and borrow_rows)


def _is_pairable_dolomite_bucket(rows: list[Any]) -> bool:
    if len(rows) <= 1 or any(row.protocol_code != "dolomite" for row in rows):
        return False
    if any(not _is_split_position_row(row) for row in rows):
        return False
    supply_rows = [row for row in rows if row.supply_usd > ZERO]
    if len(supply_rows) != 1:
        return False
    borrow_rows = [row for row in rows if row.position_key != supply_rows[0].position_key]
    if not borrow_rows:
        return False
    return all(row.borrow_usd > ZERO and row.supply_usd <= ZERO for row in borrow_rows)


def _is_pairable_zest_bucket(rows: list[Any]) -> bool:
    if len(rows) <= 1 or any(row.protocol_code != "zest" for row in rows):
        return False
    supply_rows = [row for row in rows if row.supply_usd > ZERO]
    borrow_rows = [row for row in rows if row.borrow_usd > DUST_BORROW_USD]
    return bool(supply_rows and borrow_rows)


def _is_groupable_stakedao_bucket(rows: list[Any]) -> bool:
    if len(rows) <= 1 or any(row.protocol_code != "stakedao" for row in rows):
        return False
    if any(row.market_kind != "vault_underlying" for row in rows):
        return False
    return all(row.supply_usd > ZERO and row.borrow_usd <= ZERO for row in rows)


def _singleton_position(row: Any) -> _PositionAggregate:
    supply_legs = (_build_supply_leg(row),)
    borrow_leg = _build_borrow_leg(row)
    borrow_legs = (borrow_leg,) if borrow_leg is not None else ()
    display_name = _display_name(
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        protocol_code=row.protocol_code,
        chain_code=row.chain_code,
    )
    position_kind = "Curated Vault" if row.market_kind in {"vault", "vault_underlying"} else "Lend"
    if borrow_legs or row.health_factor is not None:
        position_kind = "Carry"
    return _PositionAggregate(
        position_id=row.position_id,
        position_key=row.position_key,
        display_name=display_name,
        wallet_address=row.wallet_address,
        product_code=row.product_code,
        protocol_code=row.protocol_code,
        chain_code=row.chain_code,
        position_kind=position_kind,
        market_exposure_slug=row.market_exposure_slug,
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        net_equity_usd=row.net_equity_usd,
        leverage_ratio=row.leverage_ratio,
        health_factor=row.health_factor,
        gross_yield_daily_usd=row.gross_yield_daily_usd,
        net_yield_daily_usd=row.net_yield_daily_usd,
        gross_yield_mtd_usd=row.gross_yield_mtd_usd,
        net_yield_mtd_usd=row.net_yield_mtd_usd,
        strategy_fee_daily_usd=row.strategy_fee_daily_usd,
        avant_gop_daily_usd=row.avant_gop_daily_usd,
        strategy_fee_mtd_usd=row.strategy_fee_mtd_usd,
        avant_gop_mtd_usd=row.avant_gop_mtd_usd,
        gross_roe=row.gross_roe,
        net_roe=row.net_roe,
        member_position_keys=(row.position_key,),
    )


def _paired_reserve_position(rows: list[Any], avg_equity: dict[str, Decimal]) -> _PositionAggregate:
    supply_rows = sorted(
        (row for row in rows if row.supply_usd > ZERO),
        key=lambda row: (row.supply_usd, row.position_key),
        reverse=True,
    )
    supply_row = supply_rows[0]
    borrow_rows = sorted(
        (row for row in rows if row.borrow_usd > ZERO),
        key=lambda row: (row.borrow_usd, row.position_key),
        reverse=True,
    )
    supply_legs = tuple(_build_supply_leg(row) for row in supply_rows)
    borrow_legs = tuple(leg for row in borrow_rows if (leg := _build_borrow_leg(row)) is not None)
    display_name = _display_name(
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
    )
    gross_yield_daily_usd = sum((row.gross_yield_daily_usd for row in rows), ZERO)
    net_yield_daily_usd = sum((row.net_yield_daily_usd for row in rows), ZERO)
    gross_yield_mtd_usd = sum((row.gross_yield_mtd_usd for row in rows), ZERO)
    net_yield_mtd_usd = sum((row.net_yield_mtd_usd for row in rows), ZERO)
    strategy_fee_daily_usd = sum((row.strategy_fee_daily_usd for row in rows), ZERO)
    avant_gop_daily_usd = sum((row.avant_gop_daily_usd for row in rows), ZERO)
    strategy_fee_mtd_usd = sum((row.strategy_fee_mtd_usd for row in rows), ZERO)
    avant_gop_mtd_usd = sum((row.avant_gop_mtd_usd for row in rows), ZERO)
    total_supply_usd = sum((row.supply_usd for row in rows), ZERO)
    total_net_equity_usd = sum((row.net_equity_usd for row in rows), ZERO)
    total_avg_equity_usd = sum((avg_equity.get(row.position_key, ZERO) for row in rows), ZERO)
    health_values = [row.health_factor for row in rows if row.health_factor is not None]
    return _PositionAggregate(
        position_id=supply_row.position_id,
        position_key=_synthetic_position_key(supply_row),
        display_name=display_name,
        wallet_address=supply_row.wallet_address,
        product_code=supply_row.product_code,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
        position_kind="Carry",
        market_exposure_slug=None,
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        net_equity_usd=total_net_equity_usd,
        leverage_ratio=leverage_ratio(supply_usd=total_supply_usd, equity_usd=total_net_equity_usd),
        health_factor=min(health_values) if health_values else None,
        gross_yield_daily_usd=gross_yield_daily_usd,
        net_yield_daily_usd=net_yield_daily_usd,
        gross_yield_mtd_usd=gross_yield_mtd_usd,
        net_yield_mtd_usd=net_yield_mtd_usd,
        strategy_fee_daily_usd=strategy_fee_daily_usd,
        avant_gop_daily_usd=avant_gop_daily_usd,
        strategy_fee_mtd_usd=strategy_fee_mtd_usd,
        avant_gop_mtd_usd=avant_gop_mtd_usd,
        gross_roe=(
            gross_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None
        ),
        net_roe=net_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None,
        member_position_keys=tuple(sorted(row.position_key for row in rows)),
    )


def _paired_dolomite_position(
    rows: list[Any],
    avg_equity: dict[str, Decimal],
    *,
    account_number: str,
) -> _PositionAggregate:
    supply_row = next(row for row in rows if row.supply_usd > ZERO)
    supply_legs = (_build_supply_leg(supply_row),)
    borrow_rows = sorted(
        (
            row
            for row in rows
            if row.position_key != supply_row.position_key and row.borrow_usd > ZERO
        ),
        key=lambda row: (row.borrow_usd, row.position_key),
        reverse=True,
    )
    borrow_legs = tuple(leg for row in borrow_rows if (leg := _build_borrow_leg(row)) is not None)
    display_name = _display_name(
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
    )
    gross_yield_daily_usd = sum((row.gross_yield_daily_usd for row in rows), ZERO)
    net_yield_daily_usd = sum((row.net_yield_daily_usd for row in rows), ZERO)
    gross_yield_mtd_usd = sum((row.gross_yield_mtd_usd for row in rows), ZERO)
    net_yield_mtd_usd = sum((row.net_yield_mtd_usd for row in rows), ZERO)
    strategy_fee_daily_usd = sum((row.strategy_fee_daily_usd for row in rows), ZERO)
    avant_gop_daily_usd = sum((row.avant_gop_daily_usd for row in rows), ZERO)
    strategy_fee_mtd_usd = sum((row.strategy_fee_mtd_usd for row in rows), ZERO)
    avant_gop_mtd_usd = sum((row.avant_gop_mtd_usd for row in rows), ZERO)
    total_supply_usd = sum((row.supply_usd for row in rows), ZERO)
    total_net_equity_usd = sum((row.net_equity_usd for row in rows), ZERO)
    total_avg_equity_usd = sum((avg_equity.get(row.position_key, ZERO) for row in rows), ZERO)
    health_values = [row.health_factor for row in rows if row.health_factor is not None]
    return _PositionAggregate(
        position_id=supply_row.position_id,
        position_key=_synthetic_dolomite_position_key(supply_row, account_number=account_number),
        display_name=display_name,
        wallet_address=supply_row.wallet_address,
        product_code=supply_row.product_code,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
        position_kind="Carry",
        market_exposure_slug=None,
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        net_equity_usd=total_net_equity_usd,
        leverage_ratio=leverage_ratio(supply_usd=total_supply_usd, equity_usd=total_net_equity_usd),
        health_factor=min(health_values) if health_values else None,
        gross_yield_daily_usd=gross_yield_daily_usd,
        net_yield_daily_usd=net_yield_daily_usd,
        gross_yield_mtd_usd=gross_yield_mtd_usd,
        net_yield_mtd_usd=net_yield_mtd_usd,
        strategy_fee_daily_usd=strategy_fee_daily_usd,
        avant_gop_daily_usd=avant_gop_daily_usd,
        strategy_fee_mtd_usd=strategy_fee_mtd_usd,
        avant_gop_mtd_usd=avant_gop_mtd_usd,
        gross_roe=(
            gross_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None
        ),
        net_roe=net_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None,
        member_position_keys=tuple(sorted(row.position_key for row in rows)),
    )


def _paired_zest_position(rows: list[Any], avg_equity: dict[str, Decimal]) -> _PositionAggregate:
    supply_rows = sorted(
        (row for row in rows if row.supply_usd > ZERO),
        key=lambda row: (row.supply_usd, row.position_key),
        reverse=True,
    )
    supply_row = supply_rows[0]
    borrow_rows = sorted(
        (row for row in rows if row.borrow_usd > DUST_BORROW_USD),
        key=lambda row: (row.borrow_usd, row.position_key),
        reverse=True,
    )
    supply_legs = tuple(_build_supply_leg(row) for row in supply_rows)
    borrow_legs = tuple(leg for row in borrow_rows if (leg := _build_borrow_leg(row)) is not None)
    display_name = _display_name(
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
    )
    gross_yield_daily_usd = sum((row.gross_yield_daily_usd for row in rows), ZERO)
    net_yield_daily_usd = sum((row.net_yield_daily_usd for row in rows), ZERO)
    gross_yield_mtd_usd = sum((row.gross_yield_mtd_usd for row in rows), ZERO)
    net_yield_mtd_usd = sum((row.net_yield_mtd_usd for row in rows), ZERO)
    strategy_fee_daily_usd = sum((row.strategy_fee_daily_usd for row in rows), ZERO)
    avant_gop_daily_usd = sum((row.avant_gop_daily_usd for row in rows), ZERO)
    strategy_fee_mtd_usd = sum((row.strategy_fee_mtd_usd for row in rows), ZERO)
    avant_gop_mtd_usd = sum((row.avant_gop_mtd_usd for row in rows), ZERO)
    total_supply_usd = sum((row.supply_usd for row in rows), ZERO)
    total_net_equity_usd = sum((row.net_equity_usd for row in rows), ZERO)
    total_avg_equity_usd = sum((avg_equity.get(row.position_key, ZERO) for row in rows), ZERO)
    health_values = [row.health_factor for row in rows if row.health_factor is not None]
    return _PositionAggregate(
        position_id=supply_row.position_id,
        position_key=_synthetic_zest_position_key(supply_row),
        display_name=display_name,
        wallet_address=supply_row.wallet_address,
        product_code=supply_row.product_code,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
        position_kind="Carry",
        market_exposure_slug=None,
        supply_legs=supply_legs,
        borrow_legs=borrow_legs,
        net_equity_usd=total_net_equity_usd,
        leverage_ratio=leverage_ratio(supply_usd=total_supply_usd, equity_usd=total_net_equity_usd),
        health_factor=min(health_values) if health_values else None,
        gross_yield_daily_usd=gross_yield_daily_usd,
        net_yield_daily_usd=net_yield_daily_usd,
        gross_yield_mtd_usd=gross_yield_mtd_usd,
        net_yield_mtd_usd=net_yield_mtd_usd,
        strategy_fee_daily_usd=strategy_fee_daily_usd,
        avant_gop_daily_usd=avant_gop_daily_usd,
        strategy_fee_mtd_usd=strategy_fee_mtd_usd,
        avant_gop_mtd_usd=avant_gop_mtd_usd,
        gross_roe=(
            gross_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None
        ),
        net_roe=net_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None,
        member_position_keys=tuple(sorted(row.position_key for row in rows)),
    )


def _curated_vault_position(
    rows: list[Any],
    avg_equity: dict[str, Decimal],
    *,
    vault_address: str,
) -> _PositionAggregate:
    supply_rows = sorted(
        (row for row in rows if row.supply_usd > ZERO),
        key=lambda row: (row.supply_usd, row.position_key),
        reverse=True,
    )
    supply_row = supply_rows[0]
    supply_legs = tuple(_build_supply_leg(row) for row in supply_rows)
    display_name = _display_name(
        supply_legs=supply_legs,
        borrow_legs=(),
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
    )
    gross_yield_daily_usd = sum((row.gross_yield_daily_usd for row in rows), ZERO)
    net_yield_daily_usd = sum((row.net_yield_daily_usd for row in rows), ZERO)
    gross_yield_mtd_usd = sum((row.gross_yield_mtd_usd for row in rows), ZERO)
    net_yield_mtd_usd = sum((row.net_yield_mtd_usd for row in rows), ZERO)
    strategy_fee_daily_usd = sum((row.strategy_fee_daily_usd for row in rows), ZERO)
    avant_gop_daily_usd = sum((row.avant_gop_daily_usd for row in rows), ZERO)
    strategy_fee_mtd_usd = sum((row.strategy_fee_mtd_usd for row in rows), ZERO)
    avant_gop_mtd_usd = sum((row.avant_gop_mtd_usd for row in rows), ZERO)
    total_supply_usd = sum((row.supply_usd for row in rows), ZERO)
    total_net_equity_usd = sum((row.net_equity_usd for row in rows), ZERO)
    total_avg_equity_usd = sum((avg_equity.get(row.position_key, ZERO) for row in rows), ZERO)
    return _PositionAggregate(
        position_id=supply_row.position_id,
        position_key=_synthetic_stakedao_position_key(supply_row, vault_address=vault_address),
        display_name=display_name,
        wallet_address=supply_row.wallet_address,
        product_code=supply_row.product_code,
        protocol_code=supply_row.protocol_code,
        chain_code=supply_row.chain_code,
        position_kind="Curated Vault",
        market_exposure_slug=None,
        supply_legs=supply_legs,
        borrow_legs=(),
        net_equity_usd=total_net_equity_usd,
        leverage_ratio=leverage_ratio(supply_usd=total_supply_usd, equity_usd=total_net_equity_usd),
        health_factor=None,
        gross_yield_daily_usd=gross_yield_daily_usd,
        net_yield_daily_usd=net_yield_daily_usd,
        gross_yield_mtd_usd=gross_yield_mtd_usd,
        net_yield_mtd_usd=net_yield_mtd_usd,
        strategy_fee_daily_usd=strategy_fee_daily_usd,
        avant_gop_daily_usd=avant_gop_daily_usd,
        strategy_fee_mtd_usd=strategy_fee_mtd_usd,
        avant_gop_mtd_usd=avant_gop_mtd_usd,
        gross_roe=(
            gross_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None
        ),
        net_roe=net_yield_daily_usd / total_avg_equity_usd if total_avg_equity_usd > ZERO else None,
        member_position_keys=tuple(sorted(row.position_key for row in rows)),
    )


def _sort_value(position: _PositionAggregate, sort_by: str) -> Decimal | None:
    if sort_by == "supply_usd":
        return _total_supply_usd(position.supply_legs)
    if sort_by == "borrow_usd":
        return sum((leg.usd_value for leg in position.borrow_legs), ZERO)
    if sort_by == "gross_yield_daily_usd":
        return position.gross_yield_daily_usd
    if sort_by == "strategy_fee_daily_usd":
        return position.strategy_fee_daily_usd
    if sort_by == "avant_gop_daily_usd":
        return position.avant_gop_daily_usd
    if sort_by == "net_yield_daily_usd":
        return position.net_yield_daily_usd
    if sort_by == "gross_yield_mtd_usd":
        return position.gross_yield_mtd_usd
    if sort_by == "net_yield_mtd_usd":
        return position.net_yield_mtd_usd
    if sort_by == "gross_roe":
        return position.gross_roe
    if sort_by == "net_roe":
        return position.net_roe
    if sort_by == "health_factor":
        return position.health_factor
    return position.net_equity_usd


def _group_positions(
    session: Session,
    *,
    business_date: date,
    rows: Sequence[Any],
) -> list[_PositionAggregate]:
    avg_equity = _avg_equity_map(
        session,
        business_date=business_date,
        position_keys=[row.position_key for row in rows],
    )
    reserve_buckets: dict[tuple[str, str | None, str, str], list[Any]] = {}
    dolomite_buckets: dict[tuple[str, str | None, str, str, str], list[Any]] = {}
    zest_buckets: dict[tuple[str, str | None, str, str], list[Any]] = {}
    stakedao_buckets: dict[tuple[str, str | None, str, str, str], list[Any]] = {}
    for row in rows:
        if row.market_kind == "reserve" and _is_split_position_row(row):
            reserve_buckets.setdefault(_reserve_bucket_key(row), []).append(row)
        dolomite_bucket = _dolomite_bucket_key(row)
        if dolomite_bucket is not None and _is_split_position_row(row):
            dolomite_buckets.setdefault(dolomite_bucket, []).append(row)
        zest_bucket = _zest_bucket_key(row)
        if zest_bucket is not None:
            zest_buckets.setdefault(zest_bucket, []).append(row)
        stakedao_bucket = _stakedao_bucket_key(row)
        if stakedao_bucket is not None:
            stakedao_buckets.setdefault(stakedao_bucket, []).append(row)

    consumed: set[str] = set()
    grouped: list[_PositionAggregate] = []
    for bucket_rows in reserve_buckets.values():
        if not _is_pairable_reserve_bucket(bucket_rows):
            continue
        grouped.append(_paired_reserve_position(bucket_rows, avg_equity))
        consumed.update(row.position_key for row in bucket_rows)
    for dolomite_bucket, bucket_rows in dolomite_buckets.items():
        if not _is_pairable_dolomite_bucket(bucket_rows):
            continue
        grouped.append(
            _paired_dolomite_position(bucket_rows, avg_equity, account_number=dolomite_bucket[-1])
        )
        consumed.update(row.position_key for row in bucket_rows)
    for bucket_rows in zest_buckets.values():
        if not _is_pairable_zest_bucket(bucket_rows):
            continue
        grouped.append(_paired_zest_position(bucket_rows, avg_equity))
        consumed.update(row.position_key for row in bucket_rows)
    for stakedao_bucket, bucket_rows in stakedao_buckets.items():
        if not _is_groupable_stakedao_bucket(bucket_rows):
            continue
        grouped.append(
            _curated_vault_position(bucket_rows, avg_equity, vault_address=stakedao_bucket[-1])
        )
        consumed.update(row.position_key for row in bucket_rows)

    for row in rows:
        if row.position_key in consumed:
            continue
        grouped.append(_singleton_position(row))
    return grouped


def _sort_positions(
    positions: list[_PositionAggregate], *, sort_by: str, sort_dir: str
) -> list[_PositionAggregate]:
    reverse = sort_dir != "asc"
    non_null = [position for position in positions if _sort_value(position, sort_by) is not None]
    nulls = [position for position in positions if _sort_value(position, sort_by) is None]
    non_null.sort(
        key=lambda position: (_sort_value(position, sort_by), position.position_key),
        reverse=reverse,
    )
    return non_null + nulls


def _grouped_positions(
    session: Session,
    *,
    business_date: date,
    scope_segment: str | None = None,
    product_code: str | None = None,
    protocol_code: str | None = None,
    chain_code: str | None = None,
    wallet_address: str | None = None,
) -> list[_PositionAggregate]:
    stmt = _positions_statement().where(PortfolioPositionCurrent.business_date == business_date)
    if scope_segment is not None:
        stmt = stmt.where(PortfolioPositionCurrent.scope_segment == scope_segment)
    if product_code is not None:
        stmt = stmt.where(Product.product_code == product_code)
    if protocol_code is not None:
        stmt = stmt.where(Protocol.protocol_code == protocol_code)
    if chain_code is not None:
        stmt = stmt.where(Chain.chain_code == chain_code)
    if wallet_address is not None:
        stmt = stmt.where(Wallet.address == wallet_address)
    rows = session.execute(stmt.order_by(Position.position_id.asc())).all()
    return _group_positions(session, business_date=business_date, rows=rows)


def _find_grouped_position(
    positions: list[_PositionAggregate], *, position_key: str
) -> _PositionAggregate | None:
    for position in positions:
        if position.position_key == position_key or position_key in position.member_position_keys:
            return position
    return None


def _history_points(
    session: Session,
    *,
    member_position_keys: tuple[str, ...],
    days: int,
) -> list[PortfolioPositionHistoryPoint]:
    latest_business_date = session.scalar(
        select(func.max(PortfolioPositionDaily.business_date))
        .join(Position, Position.position_id == PortfolioPositionDaily.position_id)
        .where(Position.position_key.in_(member_position_keys))
    )
    if latest_business_date is None:
        return []

    start_date = latest_business_date.fromordinal(latest_business_date.toordinal() - days + 1)
    rows = session.execute(
        select(
            PortfolioPositionDaily.business_date,
            func.coalesce(func.sum(PortfolioPositionDaily.supply_usd), ZERO).label("supply_usd"),
            func.coalesce(func.sum(PortfolioPositionDaily.borrow_usd), ZERO).label("borrow_usd"),
            func.coalesce(func.sum(PortfolioPositionDaily.net_equity_usd), ZERO).label(
                "net_equity_usd"
            ),
            func.min(PortfolioPositionDaily.health_factor).label("health_factor"),
            func.coalesce(func.sum(PortfolioPositionDaily.gross_yield_usd), ZERO).label(
                "gross_yield_usd"
            ),
            func.coalesce(func.sum(PortfolioPositionDaily.net_yield_usd), ZERO).label(
                "net_yield_usd"
            ),
            func.coalesce(func.sum(YieldDaily.avg_equity_usd), ZERO).label("avg_equity_usd"),
        )
        .join(Position, Position.position_id == PortfolioPositionDaily.position_id)
        .outerjoin(
            YieldDaily,
            (YieldDaily.business_date == PortfolioPositionDaily.business_date)
            & (YieldDaily.position_key == Position.position_key)
            & (YieldDaily.method == METHOD),
        )
        .where(
            Position.position_key.in_(member_position_keys),
            PortfolioPositionDaily.business_date >= start_date,
            PortfolioPositionDaily.business_date <= latest_business_date,
        )
        .group_by(PortfolioPositionDaily.business_date)
        .order_by(PortfolioPositionDaily.business_date.asc())
    ).all()
    history: list[PortfolioPositionHistoryPoint] = []
    for row in rows:
        avg_equity_usd = row.avg_equity_usd or ZERO
        history.append(
            PortfolioPositionHistoryPoint(
                business_date=row.business_date,
                supply_usd=row.supply_usd,
                borrow_usd=row.borrow_usd,
                net_equity_usd=row.net_equity_usd,
                leverage_ratio=leverage_ratio(
                    supply_usd=row.supply_usd,
                    equity_usd=row.net_equity_usd,
                ),
                health_factor=row.health_factor,
                gross_yield_usd=row.gross_yield_usd,
                net_yield_usd=row.net_yield_usd,
                roe=_roe_metrics(
                    gross_roe=(
                        row.gross_yield_usd / avg_equity_usd if avg_equity_usd > ZERO else None
                    ),
                    net_roe=row.net_yield_usd / avg_equity_usd if avg_equity_usd > ZERO else None,
                ),
            )
        )
    return history


def _summary_from_positions(
    *, business_date: date, positions: list[_PositionAggregate]
) -> PortfolioSummaryResponse:
    total_supply_usd = sum(
        (_total_supply_usd(position.supply_legs) for position in positions),
        ZERO,
    )
    total_borrow_usd = sum(
        (sum((leg.usd_value for leg in position.borrow_legs), ZERO) for position in positions),
        ZERO,
    )
    total_net_equity_usd = sum((position.net_equity_usd for position in positions), ZERO)
    total_gross_yield_daily_usd = sum(
        (position.gross_yield_daily_usd for position in positions), ZERO
    )
    total_net_yield_daily_usd = sum((position.net_yield_daily_usd for position in positions), ZERO)
    total_gross_yield_mtd_usd = sum((position.gross_yield_mtd_usd for position in positions), ZERO)
    total_net_yield_mtd_usd = sum((position.net_yield_mtd_usd for position in positions), ZERO)
    total_strategy_fee_daily_usd = sum(
        (position.strategy_fee_daily_usd for position in positions), ZERO
    )
    total_avant_gop_daily_usd = sum((position.avant_gop_daily_usd for position in positions), ZERO)
    total_strategy_fee_mtd_usd = sum(
        (position.strategy_fee_mtd_usd for position in positions), ZERO
    )
    total_avant_gop_mtd_usd = sum((position.avant_gop_mtd_usd for position in positions), ZERO)
    leverage_values = [position.leverage_ratio for position in positions if position.leverage_ratio]
    avg_leverage_ratio = (
        sum((value for value in leverage_values), ZERO) / Decimal(len(leverage_values))
        if leverage_values
        else None
    )
    aggregate_roe_daily = _normalized_roe(
        total_gross_yield_daily_usd / total_net_equity_usd if total_net_equity_usd > ZERO else None
    )
    return PortfolioSummaryResponse(
        business_date=business_date,
        scope_segment="strategy_only",
        total_supply_usd=total_supply_usd,
        total_borrow_usd=total_borrow_usd,
        total_net_equity_usd=total_net_equity_usd,
        aggregate_roe_daily=aggregate_roe_daily,
        aggregate_roe_annualized=_annualize_daily_roe(aggregate_roe_daily),
        total_gross_yield_daily_usd=total_gross_yield_daily_usd,
        total_net_yield_daily_usd=total_net_yield_daily_usd,
        total_gross_yield_mtd_usd=total_gross_yield_mtd_usd,
        total_net_yield_mtd_usd=total_net_yield_mtd_usd,
        total_strategy_fee_daily_usd=total_strategy_fee_daily_usd,
        total_avant_gop_daily_usd=total_avant_gop_daily_usd,
        total_strategy_fee_mtd_usd=total_strategy_fee_mtd_usd,
        total_avant_gop_mtd_usd=total_avant_gop_mtd_usd,
        avg_leverage_ratio=avg_leverage_ratio,
        open_position_count=len(positions),
    )


@router.get("/positions/current")
def get_current_positions(
    product_code: str | None = Query(default=None),
    protocol_code: str | None = Query(default=None),
    chain_code: str | None = Query(default=None),
    wallet_address: str | None = Query(default=None),
    sort_by: str = Query(default="net_equity_usd"),
    sort_dir: str = Query(default="desc"),
    session: Session = Depends(get_session),
) -> PortfolioPositionsResponse:
    business_date = _latest_business_date(session)
    if business_date is None:
        return PortfolioPositionsResponse(business_date=date.today(), total_count=0, positions=[])
    rows = _sort_positions(
        _grouped_positions(
            session,
            business_date=business_date,
            product_code=product_code,
            protocol_code=protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
        ),
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return PortfolioPositionsResponse(
        business_date=business_date,
        total_count=len(rows),
        positions=[_build_position_row(position) for position in rows],
    )


@router.get("/positions/{position_key}/history")
def get_position_history(
    position_key: str,
    days: int = Query(default=30, ge=1, le=365),
    session: Session = Depends(get_session),
) -> PortfolioPositionDetailResponse:
    latest_business_date = _latest_business_date(session)
    if latest_business_date is None:
        raise HTTPException(status_code=404, detail="position not found")
    current_position = _find_grouped_position(
        _grouped_positions(session, business_date=latest_business_date),
        position_key=position_key,
    )
    if current_position is None:
        raise HTTPException(status_code=404, detail="position not found")

    return PortfolioPositionDetailResponse(
        position=_build_position_row(current_position),
        history=_history_points(
            session,
            member_position_keys=current_position.member_position_keys,
            days=days,
        ),
    )


@router.get("/summary")
def get_portfolio_summary(session: Session = Depends(get_session)) -> PortfolioSummaryResponse:
    business_date = _latest_business_date(session)
    if business_date is None:
        today = datetime.now(UTC).date()
        return _summary_from_positions(business_date=today, positions=[])
    row = session.scalar(
        select(PortfolioSummaryDaily).where(
            PortfolioSummaryDaily.business_date == business_date,
            PortfolioSummaryDaily.scope_segment == "strategy_only",
        )
    )
    if row is None:
        return _summary_from_positions(
            business_date=business_date,
            positions=_grouped_positions(
                session,
                business_date=business_date,
                scope_segment="strategy_only",
            ),
        )
    return PortfolioSummaryResponse(
        business_date=row.business_date,
        scope_segment=row.scope_segment,
        total_supply_usd=row.total_supply_usd,
        total_borrow_usd=row.total_borrow_usd,
        total_net_equity_usd=row.total_net_equity_usd,
        aggregate_roe_daily=_normalized_roe(row.aggregate_roe),
        aggregate_roe_annualized=_annualize_daily_roe(row.aggregate_roe),
        total_gross_yield_daily_usd=row.total_gross_yield_daily_usd,
        total_net_yield_daily_usd=row.total_net_yield_daily_usd,
        total_gross_yield_mtd_usd=row.total_gross_yield_mtd_usd,
        total_net_yield_mtd_usd=row.total_net_yield_mtd_usd,
        total_strategy_fee_daily_usd=row.total_strategy_fee_daily_usd,
        total_avant_gop_daily_usd=row.total_avant_gop_daily_usd,
        total_strategy_fee_mtd_usd=row.total_strategy_fee_mtd_usd,
        total_avant_gop_mtd_usd=row.total_avant_gop_mtd_usd,
        avg_leverage_ratio=row.avg_leverage_ratio,
        open_position_count=row.open_position_count,
    )
