"""Served portfolio table builders for the executive dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import case, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased

from analytics.fee_engine import apply_fee_waterfall
from analytics.yield_engine import denver_business_bounds_utc
from core.dashboard_contracts import leverage_ratio
from core.db.models import (
    Chain,
    PortfolioPositionCurrent,
    PortfolioPositionDaily,
    PortfolioSummaryDaily,
    Position,
    PositionSnapshot,
    PositionSnapshotLeg,
    Product,
    Protocol,
    Token,
    Wallet,
    YieldDaily,
)
from core.position_backfill import backfill_positions_and_legs

ZERO = Decimal("0")
METHOD = "apy_prorated_sod_eod"


@dataclass(frozen=True)
class PortfolioBuildSummary:
    business_date: date
    current_rows_written: int
    daily_rows_written: int
    summary_rows_written: int


SUPPLY_LEG = aliased(PositionSnapshotLeg)
BORROW_LEG = aliased(PositionSnapshotLeg)
SUPPLY_TOKEN = aliased(Token)
BORROW_TOKEN = aliased(Token)


class PortfolioViewEngine:
    """Build persisted served portfolio views from canonical position snapshots and yield facts."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def compute_daily(
        self,
        *,
        business_date: date,
        as_of_ts_utc: datetime | None = None,
    ) -> PortfolioBuildSummary:
        daily_as_of = as_of_ts_utc or self._resolve_business_date_snapshot(business_date)
        current_as_of = self._latest_snapshot_ts()
        backfill_timestamps = {
            snapshot_ts for snapshot_ts in (daily_as_of, current_as_of) if snapshot_ts is not None
        }
        for snapshot_ts in backfill_timestamps:
            backfill_positions_and_legs(self.session, as_of_ts_utc=snapshot_ts)

        daily_rows = self._build_rows(
            business_date=business_date,
            as_of_ts_utc=daily_as_of,
            include_mtd=False,
        )
        self._replace_daily_rows(business_date=business_date, rows=daily_rows)
        summary_rows = self._build_summary_rows(business_date=business_date, rows=daily_rows)
        self._replace_summary_rows(business_date=business_date, rows=summary_rows)

        current_rows: list[dict[str, object]] = []
        if current_as_of is not None:
            latest_business_date = self._latest_yield_business_date() or business_date
            current_rows = self._build_rows(
                business_date=latest_business_date,
                as_of_ts_utc=current_as_of,
                include_mtd=True,
            )
            self._replace_current_rows(rows=current_rows)
        else:
            self.session.execute(delete(PortfolioPositionCurrent))

        return PortfolioBuildSummary(
            business_date=business_date,
            current_rows_written=len(current_rows),
            daily_rows_written=len(daily_rows),
            summary_rows_written=len(summary_rows),
        )

    def _latest_snapshot_ts(self) -> datetime | None:
        return self.session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))

    def _latest_yield_business_date(self) -> date | None:
        return self.session.scalar(select(func.max(YieldDaily.business_date)))

    def _resolve_business_date_snapshot(self, business_date: date) -> datetime | None:
        start_utc, end_utc = denver_business_bounds_utc(business_date)
        in_day = self.session.scalar(
            select(func.max(PositionSnapshot.as_of_ts_utc)).where(
                PositionSnapshot.as_of_ts_utc >= start_utc,
                PositionSnapshot.as_of_ts_utc < end_utc,
            )
        )
        if in_day is not None:
            return in_day
        return self.session.scalar(
            select(func.max(PositionSnapshot.as_of_ts_utc)).where(
                PositionSnapshot.as_of_ts_utc <= end_utc
            )
        )

    def _daily_yield_map(self, business_date: date) -> dict[str, YieldDaily]:
        rows = self.session.scalars(
            select(YieldDaily).where(
                YieldDaily.business_date == business_date,
                YieldDaily.position_key.is_not(None),
                YieldDaily.method == METHOD,
            )
        ).all()
        return {row.position_key: row for row in rows if row.position_key is not None}

    def _mtd_yield_map(self, business_date: date) -> dict[str, dict[str, Decimal]]:
        month_start = business_date.replace(day=1)
        rows = self.session.execute(
            select(
                YieldDaily.position_key,
                func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            )
            .where(
                YieldDaily.position_key.is_not(None),
                YieldDaily.method == METHOD,
                YieldDaily.business_date >= month_start,
                YieldDaily.business_date <= business_date,
            )
            .group_by(YieldDaily.position_key)
        ).all()
        return {
            position_key: {
                "gross_yield_mtd_usd": gross_yield_mtd_usd,
                "net_yield_mtd_usd": net_yield_mtd_usd,
                "strategy_fee_mtd_usd": strategy_fee_mtd_usd,
                "avant_gop_mtd_usd": avant_gop_mtd_usd,
            }
            for (
                position_key,
                gross_yield_mtd_usd,
                net_yield_mtd_usd,
                strategy_fee_mtd_usd,
                avant_gop_mtd_usd,
            ) in rows
            if position_key is not None
        }

    def _build_rows(
        self,
        *,
        business_date: date,
        as_of_ts_utc: datetime | None,
        include_mtd: bool,
    ) -> list[dict[str, object]]:
        if as_of_ts_utc is None:
            return []
        daily_yield = self._daily_yield_map(business_date)
        mtd_yield = self._mtd_yield_map(business_date) if include_mtd else {}
        economic_supply_amount = case(
            (
                (PositionSnapshot.collateral_usd.is_not(None))
                & (PositionSnapshot.collateral_usd > ZERO),
                PositionSnapshot.collateral_amount,
            ),
            else_=PositionSnapshot.supplied_amount,
        )
        economic_supply_usd = case(
            (
                (PositionSnapshot.collateral_usd.is_not(None))
                & (PositionSnapshot.collateral_usd > ZERO),
                PositionSnapshot.collateral_usd,
            ),
            else_=PositionSnapshot.supplied_usd,
        )

        rows = self.session.execute(
            select(
                Position.position_id,
                Position.position_key,
                Position.display_name,
                Position.market_exposure_id,
                Wallet.wallet_id,
                Wallet.address.label("wallet_address"),
                Wallet.wallet_type,
                Product.product_id,
                Product.product_code,
                Protocol.protocol_id,
                Protocol.protocol_code,
                Chain.chain_id,
                Chain.chain_code,
                economic_supply_amount.label("supply_amount"),
                economic_supply_usd.label("supply_usd"),
                PositionSnapshot.borrowed_amount,
                PositionSnapshot.borrowed_usd,
                PositionSnapshot.supply_apy,
                PositionSnapshot.reward_apy,
                PositionSnapshot.borrow_apy,
                PositionSnapshot.equity_usd,
                PositionSnapshot.health_factor,
                SUPPLY_LEG.token_id.label("supply_token_id"),
                SUPPLY_TOKEN.symbol.label("supply_token_symbol"),
                BORROW_LEG.token_id.label("borrow_token_id"),
                BORROW_TOKEN.symbol.label("borrow_token_symbol"),
            )
            .join(Position, Position.position_id == PositionSnapshot.position_id)
            .join(Wallet, Wallet.wallet_id == Position.wallet_id)
            .join(Protocol, Protocol.protocol_id == Position.protocol_id)
            .join(Chain, Chain.chain_id == Position.chain_id)
            .outerjoin(Product, Product.product_id == Position.product_id)
            .outerjoin(
                SUPPLY_LEG,
                (SUPPLY_LEG.snapshot_id == PositionSnapshot.snapshot_id)
                & (SUPPLY_LEG.leg_type == "supply"),
            )
            .outerjoin(SUPPLY_TOKEN, SUPPLY_TOKEN.token_id == SUPPLY_LEG.token_id)
            .outerjoin(
                BORROW_LEG,
                (BORROW_LEG.snapshot_id == PositionSnapshot.snapshot_id)
                & (BORROW_LEG.leg_type == "borrow"),
            )
            .outerjoin(BORROW_TOKEN, BORROW_TOKEN.token_id == BORROW_LEG.token_id)
            .where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                Position.exposure_class == "core_lending",
            )
            .order_by(Position.position_id.asc())
        ).all()

        output: list[dict[str, object]] = []
        for row in rows:
            yd = daily_yield.get(row.position_key)
            gross_yield_daily = yd.gross_yield_usd if yd else ZERO
            net_yield_daily = yd.net_yield_usd if yd else ZERO
            strategy_fee_daily = yd.strategy_fee_usd if yd else ZERO
            avant_gop_daily = yd.avant_gop_usd if yd else ZERO
            mtd = mtd_yield.get(
                row.position_key,
                {
                    "gross_yield_mtd_usd": gross_yield_daily,
                    "net_yield_mtd_usd": net_yield_daily,
                    "strategy_fee_mtd_usd": strategy_fee_daily,
                    "avant_gop_mtd_usd": avant_gop_daily,
                },
            )
            scope_segment = "customer_only" if row.wallet_type == "customer" else "strategy_only"
            output.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "position_id": row.position_id,
                    "position_key": row.position_key,
                    "display_name": row.display_name,
                    "wallet_id": row.wallet_id,
                    "wallet_address": row.wallet_address,
                    "product_id": row.product_id,
                    "product_code": row.product_code,
                    "protocol_id": row.protocol_id,
                    "protocol_code": row.protocol_code,
                    "chain_id": row.chain_id,
                    "chain_code": row.chain_code,
                    "market_exposure_id": row.market_exposure_id,
                    "scope_segment": scope_segment,
                    "supply_token_id": row.supply_token_id,
                    "supply_symbol": row.supply_token_symbol,
                    "borrow_token_id": row.borrow_token_id,
                    "borrow_symbol": row.borrow_token_symbol,
                    "supply_amount": row.supply_amount,
                    "supply_usd": row.supply_usd,
                    "supply_apy": row.supply_apy,
                    "borrow_amount": row.borrowed_amount,
                    "borrow_usd": row.borrowed_usd,
                    "borrow_apy": row.borrow_apy,
                    "reward_apy": row.reward_apy,
                    "net_equity_usd": row.equity_usd,
                    "leverage_ratio": leverage_ratio(
                        supply_usd=row.supply_usd, equity_usd=row.equity_usd
                    ),
                    "health_factor": row.health_factor,
                    "gross_yield_daily_usd": gross_yield_daily,
                    "net_yield_daily_usd": net_yield_daily,
                    "gross_yield_mtd_usd": mtd["gross_yield_mtd_usd"],
                    "net_yield_mtd_usd": mtd["net_yield_mtd_usd"],
                    "strategy_fee_daily_usd": strategy_fee_daily,
                    "avant_gop_daily_usd": avant_gop_daily,
                    "strategy_fee_mtd_usd": mtd["strategy_fee_mtd_usd"],
                    "avant_gop_mtd_usd": mtd["avant_gop_mtd_usd"],
                    "gross_roe": yd.gross_roe if yd else None,
                    "net_roe": yd.net_roe if yd else None,
                }
            )
        return output

    def _replace_current_rows(self, *, rows: list[dict[str, object]]) -> None:
        self.session.execute(delete(PortfolioPositionCurrent))
        if not rows:
            return
        current_rows = [
            {
                "position_id": row["position_id"],
                "business_date": row["business_date"],
                "as_of_ts_utc": row["as_of_ts_utc"],
                "wallet_id": row["wallet_id"],
                "product_id": row["product_id"],
                "protocol_id": row["protocol_id"],
                "chain_id": row["chain_id"],
                "market_exposure_id": row["market_exposure_id"],
                "scope_segment": row["scope_segment"],
                "supply_token_id": row["supply_token_id"],
                "borrow_token_id": row["borrow_token_id"],
                "supply_amount": row["supply_amount"],
                "supply_usd": row["supply_usd"],
                "supply_apy": row["supply_apy"],
                "borrow_amount": row["borrow_amount"],
                "borrow_usd": row["borrow_usd"],
                "borrow_apy": row["borrow_apy"],
                "reward_apy": row["reward_apy"],
                "net_equity_usd": row["net_equity_usd"],
                "leverage_ratio": row["leverage_ratio"],
                "health_factor": row["health_factor"],
                "gross_yield_daily_usd": row["gross_yield_daily_usd"],
                "net_yield_daily_usd": row["net_yield_daily_usd"],
                "gross_yield_mtd_usd": row["gross_yield_mtd_usd"],
                "net_yield_mtd_usd": row["net_yield_mtd_usd"],
                "strategy_fee_daily_usd": row["strategy_fee_daily_usd"],
                "avant_gop_daily_usd": row["avant_gop_daily_usd"],
                "strategy_fee_mtd_usd": row["strategy_fee_mtd_usd"],
                "avant_gop_mtd_usd": row["avant_gop_mtd_usd"],
                "gross_roe": row["gross_roe"],
                "net_roe": row["net_roe"],
            }
            for row in rows
        ]
        self.session.execute(insert(PortfolioPositionCurrent).values(current_rows))

    def _replace_daily_rows(self, *, business_date: date, rows: list[dict[str, object]]) -> None:
        self.session.execute(
            delete(PortfolioPositionDaily).where(
                PortfolioPositionDaily.business_date == business_date
            )
        )
        if not rows:
            return
        daily_rows = [
            {
                "business_date": row["business_date"],
                "position_id": row["position_id"],
                "as_of_ts_utc": row["as_of_ts_utc"],
                "market_exposure_id": row["market_exposure_id"],
                "scope_segment": row["scope_segment"],
                "supply_usd": row["supply_usd"],
                "borrow_usd": row["borrow_usd"],
                "net_equity_usd": row["net_equity_usd"],
                "leverage_ratio": row["leverage_ratio"],
                "health_factor": row["health_factor"],
                "gross_yield_usd": row["gross_yield_daily_usd"],
                "net_yield_usd": row["net_yield_daily_usd"],
                "strategy_fee_usd": row["strategy_fee_daily_usd"],
                "avant_gop_usd": row["avant_gop_daily_usd"],
                "gross_roe": row["gross_roe"],
                "net_roe": row["net_roe"],
            }
            for row in rows
        ]
        self.session.execute(insert(PortfolioPositionDaily).values(daily_rows))

    def _build_summary_rows(
        self, *, business_date: date, rows: list[dict[str, object]]
    ) -> list[dict[str, object]]:
        if not rows:
            return []
        grouped: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            grouped.setdefault(str(row["scope_segment"]), []).append(row)

        month_start = business_date.replace(day=1)
        month_to_date_yield_row = self.session.execute(
            select(
                func.coalesce(func.sum(YieldDaily.gross_yield_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.net_yield_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.strategy_fee_usd), ZERO),
                func.coalesce(func.sum(YieldDaily.avant_gop_usd), ZERO),
            ).where(
                YieldDaily.business_date >= month_start,
                YieldDaily.business_date <= business_date,
                YieldDaily.position_key.is_(None),
                YieldDaily.wallet_id.is_(None),
                YieldDaily.product_id.is_(None),
                YieldDaily.protocol_id.is_(None),
                YieldDaily.method == METHOD,
            )
        ).one()

        summaries: list[dict[str, object]] = []
        for scope_segment, scope_rows in grouped.items():
            total_supply_usd = sum((Decimal(str(row["supply_usd"])) for row in scope_rows), ZERO)
            total_borrow_usd = sum((Decimal(str(row["borrow_usd"])) for row in scope_rows), ZERO)
            total_net_equity_usd = sum(
                (Decimal(str(row["net_equity_usd"])) for row in scope_rows), ZERO
            )
            total_gross_yield_daily_usd = sum(
                (Decimal(str(row["gross_yield_daily_usd"])) for row in scope_rows), ZERO
            )
            daily_fees = apply_fee_waterfall(total_gross_yield_daily_usd)
            total_net_yield_daily_usd = daily_fees.net_yield_usd
            total_strategy_fee_daily_usd = daily_fees.strategy_fee_usd
            total_avant_gop_daily_usd = daily_fees.avant_gop_usd
            (
                total_gross_yield_mtd_usd,
                total_net_yield_mtd_usd,
                total_strategy_fee_mtd_usd,
                total_avant_gop_mtd_usd,
            ) = month_to_date_yield_row
            leverage_values = [
                row["leverage_ratio"] for row in scope_rows if row["leverage_ratio"] is not None
            ]
            avg_leverage_ratio = (
                sum((Decimal(str(value)) for value in leverage_values), ZERO)
                / Decimal(len(leverage_values))
                if leverage_values
                else None
            )
            aggregate_roe = (
                total_gross_yield_daily_usd / total_net_equity_usd
                if total_net_equity_usd > ZERO
                else None
            )
            summaries.append(
                {
                    "business_date": business_date,
                    "scope_segment": scope_segment,
                    "total_supply_usd": total_supply_usd,
                    "total_borrow_usd": total_borrow_usd,
                    "total_net_equity_usd": total_net_equity_usd,
                    "aggregate_roe": aggregate_roe,
                    "total_gross_yield_daily_usd": total_gross_yield_daily_usd,
                    "total_net_yield_daily_usd": total_net_yield_daily_usd,
                    "total_gross_yield_mtd_usd": total_gross_yield_mtd_usd,
                    "total_net_yield_mtd_usd": total_net_yield_mtd_usd,
                    "total_strategy_fee_daily_usd": total_strategy_fee_daily_usd,
                    "total_avant_gop_daily_usd": total_avant_gop_daily_usd,
                    "total_strategy_fee_mtd_usd": total_strategy_fee_mtd_usd,
                    "total_avant_gop_mtd_usd": total_avant_gop_mtd_usd,
                    "avg_leverage_ratio": avg_leverage_ratio,
                    "open_position_count": len(scope_rows),
                }
            )
        return summaries

    def _replace_summary_rows(self, *, business_date: date, rows: list[dict[str, object]]) -> None:
        self.session.execute(
            delete(PortfolioSummaryDaily).where(
                PortfolioSummaryDaily.business_date == business_date
            )
        )
        if not rows:
            return
        self.session.execute(insert(PortfolioSummaryDaily).values(rows))
