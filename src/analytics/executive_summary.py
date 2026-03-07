"""Executive summary served table builder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.db.models import Alert, ExecutiveSummaryDaily, MarketSummaryDaily, PortfolioSummaryDaily

ZERO = Decimal("0")


@dataclass(frozen=True)
class ExecutiveSummaryBuildSummary:
    business_date: date
    rows_written: int


class ExecutiveSummaryEngine:
    """Build persisted executive summary rows from served portfolio and market rollups."""

    def __init__(self, session: Session) -> None:
        self.session = session

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
        if portfolio is None and markets is None:
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
            "customer_metrics_ready": False,
        }
        stmt = insert(ExecutiveSummaryDaily).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=[ExecutiveSummaryDaily.business_date],
            set_={
                "nav_usd": stmt.excluded.nav_usd,
                "portfolio_net_equity_usd": stmt.excluded.portfolio_net_equity_usd,
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
