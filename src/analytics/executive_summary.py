"""Executive summary served table builder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Alert,
    ConsumerCohortDaily,
    ConsumerMarketDemandDaily,
    ExecutiveSummaryDaily,
    HolderBehaviorDaily,
    HolderScorecardDaily,
    Market,
    MarketSummaryDaily,
    PortfolioPositionDaily,
    PortfolioSummaryDaily,
    PositionSnapshot,
    Wallet,
)

ZERO = Decimal("0")


@dataclass(frozen=True)
class ExecutiveSummaryBuildSummary:
    business_date: date
    rows_written: int


class ExecutiveSummaryEngine:
    """Build persisted executive summary rows from served portfolio and market rollups."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def _resolve_business_date_snapshot(self, *, business_date: date) -> datetime | None:
        summary_snapshot = self.session.scalar(
            select(func.max(PortfolioPositionDaily.as_of_ts_utc)).where(
                PortfolioPositionDaily.business_date == business_date
            )
        )
        if summary_snapshot is not None:
            return summary_snapshot

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

    def _market_stability_ops_net_equity(self, *, business_date: date) -> Decimal:
        snapshot_ts = self._resolve_business_date_snapshot(business_date=business_date)
        if snapshot_ts is None:
            return ZERO

        return (
            self.session.scalar(
                select(func.coalesce(func.sum(PositionSnapshot.equity_usd), ZERO))
                .join(Market, Market.market_id == PositionSnapshot.market_id)
                .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
                .where(
                    PositionSnapshot.as_of_ts_utc == snapshot_ts,
                    Wallet.wallet_type == "strategy",
                    Market.metadata_json["capital_bucket"].as_string() == "market_stability_ops",
                )
            )
            or ZERO
        )

    def compute_daily(self, *, business_date: date) -> ExecutiveSummaryBuildSummary:
        portfolio = self.session.scalar(
            select(PortfolioSummaryDaily).where(
                PortfolioSummaryDaily.business_date == business_date,
                PortfolioSummaryDaily.scope_segment == "strategy_only",
            )
        )
        markets = self.session.scalar(
            select(MarketSummaryDaily).where(
                MarketSummaryDaily.business_date == business_date,
                MarketSummaryDaily.scope_segment == "strategy_only",
            )
        )
        market_stability_ops_net_equity = self._market_stability_ops_net_equity(
            business_date=business_date
        )
        if portfolio is None and markets is None and market_stability_ops_net_equity == ZERO:
            self.session.execute(
                delete(ExecutiveSummaryDaily).where(
                    ExecutiveSummaryDaily.business_date == business_date
                )
            )
            return ExecutiveSummaryBuildSummary(business_date=business_date, rows_written=0)

        open_alert_count = int(
            self.session.scalar(select(func.count(Alert.alert_id)).where(Alert.status == "open"))
            or 0
        )
        row = {
            "business_date": business_date,
            "nav_usd": portfolio.total_net_equity_usd if portfolio is not None else ZERO,
            "portfolio_net_equity_usd": (
                portfolio.total_net_equity_usd if portfolio is not None else ZERO
            ),
            "market_stability_ops_net_equity_usd": market_stability_ops_net_equity,
            "portfolio_aggregate_roe": portfolio.aggregate_roe if portfolio is not None else None,
            "total_gross_yield_daily_usd": (
                portfolio.total_gross_yield_daily_usd if portfolio is not None else ZERO
            ),
            "total_net_yield_daily_usd": (
                portfolio.total_net_yield_daily_usd if portfolio is not None else ZERO
            ),
            "total_gross_yield_mtd_usd": (
                portfolio.total_gross_yield_mtd_usd if portfolio is not None else ZERO
            ),
            "total_net_yield_mtd_usd": (
                portfolio.total_net_yield_mtd_usd if portfolio is not None else ZERO
            ),
            "total_strategy_fee_daily_usd": (
                portfolio.total_strategy_fee_daily_usd if portfolio is not None else ZERO
            ),
            "total_avant_gop_daily_usd": (
                portfolio.total_avant_gop_daily_usd if portfolio is not None else ZERO
            ),
            "total_strategy_fee_mtd_usd": (
                portfolio.total_strategy_fee_mtd_usd if portfolio is not None else ZERO
            ),
            "total_avant_gop_mtd_usd": (
                portfolio.total_avant_gop_mtd_usd if portfolio is not None else ZERO
            ),
            "market_total_supply_usd": markets.total_supply_usd if markets is not None else ZERO,
            "market_total_borrow_usd": markets.total_borrow_usd if markets is not None else ZERO,
            "markets_at_risk_count": markets.markets_at_risk_count if markets is not None else 0,
            "open_alert_count": open_alert_count,
            "customer_metrics_ready": self._customer_metrics_ready(business_date=business_date),
        }
        stmt = insert(ExecutiveSummaryDaily).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=[ExecutiveSummaryDaily.business_date],
            set_={
                "nav_usd": stmt.excluded.nav_usd,
                "portfolio_net_equity_usd": stmt.excluded.portfolio_net_equity_usd,
                "market_stability_ops_net_equity_usd": (
                    stmt.excluded.market_stability_ops_net_equity_usd
                ),
                "portfolio_aggregate_roe": stmt.excluded.portfolio_aggregate_roe,
                "total_gross_yield_daily_usd": stmt.excluded.total_gross_yield_daily_usd,
                "total_net_yield_daily_usd": stmt.excluded.total_net_yield_daily_usd,
                "total_gross_yield_mtd_usd": stmt.excluded.total_gross_yield_mtd_usd,
                "total_net_yield_mtd_usd": stmt.excluded.total_net_yield_mtd_usd,
                "total_strategy_fee_daily_usd": stmt.excluded.total_strategy_fee_daily_usd,
                "total_avant_gop_daily_usd": stmt.excluded.total_avant_gop_daily_usd,
                "total_strategy_fee_mtd_usd": stmt.excluded.total_strategy_fee_mtd_usd,
                "total_avant_gop_mtd_usd": stmt.excluded.total_avant_gop_mtd_usd,
                "market_total_supply_usd": stmt.excluded.market_total_supply_usd,
                "market_total_borrow_usd": stmt.excluded.market_total_borrow_usd,
                "markets_at_risk_count": stmt.excluded.markets_at_risk_count,
                "open_alert_count": stmt.excluded.open_alert_count,
                "customer_metrics_ready": stmt.excluded.customer_metrics_ready,
            },
        )
        self.session.execute(stmt)
        return ExecutiveSummaryBuildSummary(business_date=business_date, rows_written=1)

    def _customer_metrics_ready(self, *, business_date: date) -> bool:
        required_tables_ready = (
            self.session.scalar(
                select(func.count())
                .select_from(ConsumerCohortDaily)
                .where(ConsumerCohortDaily.business_date == business_date)
            )
            or 0
        )
        if required_tables_ready == 0:
            return False

        holder_rows = (
            self.session.scalar(
                select(func.count())
                .select_from(HolderBehaviorDaily)
                .where(HolderBehaviorDaily.business_date == business_date)
            )
            or 0
        )
        demand_rows = (
            self.session.scalar(
                select(func.count())
                .select_from(ConsumerMarketDemandDaily)
                .where(ConsumerMarketDemandDaily.business_date == business_date)
            )
            or 0
        )
        scorecard_rows = (
            self.session.scalar(
                select(func.count())
                .select_from(HolderScorecardDaily)
                .where(HolderScorecardDaily.business_date == business_date)
            )
            or 0
        )
        return holder_rows > 0 and demand_rows > 0 and scorecard_rows > 0
