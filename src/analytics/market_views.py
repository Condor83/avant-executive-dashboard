"""Served markets table builders for the executive dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.alerts import AlertEngine
from analytics.market_engine import MarketEngine
from analytics.market_exposures import (
    build_market_exposure_usage_metrics,
    ensure_market_exposures,
)
from analytics.risk_engine import RiskComputationResult, RiskEngine
from core.config import RiskThresholdsConfig
from core.db.models import (
    Alert,
    MarketExposure,
    MarketExposureComponent,
    MarketExposureDaily,
    MarketHealthDaily,
    MarketOverviewDaily,
    MarketSummaryDaily,
    Token,
)
from core.yields import AVANT_APY_ENDPOINTS

ZERO = Decimal("0")
SEVERITY_RANK = {"normal": 0, "watch": 1, "elevated": 2, "critical": 3}
ACTIVE_ALERT_STATUSES = ("open", "ack")


@dataclass(frozen=True)
class MarketViewBuildSummary:
    business_date: date
    health_rows_written: int
    exposure_rows_written: int
    summary_rows_written: int


class _CollateralYieldOracle(Protocol):
    def get_token_apy(self, symbol: str) -> Decimal:
        """Return token APY in 0.0-1.0 units."""


class MarketViewEngine:
    """Build persisted served markets views from market snapshots, risk, and portfolio usage."""

    def __init__(
        self,
        session: Session,
        *,
        thresholds: RiskThresholdsConfig | None = None,
        avant_yield_oracle: _CollateralYieldOracle | None = None,
    ) -> None:
        self.session = session
        self.thresholds = thresholds
        self.avant_yield_oracle = avant_yield_oracle

    def compute_daily(self, *, business_date: date) -> MarketViewBuildSummary:
        ensure_market_exposures(self.session)
        MarketEngine(self.session).compute_daily(business_date=business_date)
        risk_result = self._risk_result_for_date(business_date)
        health_rows = self._build_market_health_rows(
            business_date=business_date, risk_result=risk_result
        )
        self._replace_market_health_rows(business_date=business_date, rows=health_rows)

        exposure_rows = self._build_exposure_rows(business_date=business_date)
        self._replace_exposure_rows(business_date=business_date, rows=exposure_rows)
        summary_rows = self._build_summary_rows(business_date=business_date, rows=exposure_rows)
        self._replace_summary_rows(business_date=business_date, rows=summary_rows)

        return MarketViewBuildSummary(
            business_date=business_date,
            health_rows_written=len(health_rows),
            exposure_rows_written=len(exposure_rows),
            summary_rows_written=len(summary_rows),
        )

    def _risk_result_for_date(self, business_date: date) -> RiskComputationResult | None:
        if self.thresholds is None:
            return None
        return RiskEngine(self.session, thresholds=self.thresholds).compute_for_date(
            business_date=business_date
        )

    def _candidate_map(self, risk_result: RiskComputationResult | None) -> dict[int, str]:
        if risk_result is None or self.thresholds is None:
            return {}
        candidates = AlertEngine(self.session, thresholds=self.thresholds).build_candidates(
            risk_result
        )
        severity_by_market: dict[int, str] = {}
        for candidate in candidates:
            if candidate.entity_type != "market":
                continue
            try:
                market_id = int(candidate.entity_id)
            except ValueError:
                continue
            current = severity_by_market.get(market_id)
            candidate_status = self._severity_to_status(candidate.severity)
            if current is None or SEVERITY_RANK[candidate_status] > SEVERITY_RANK[current]:
                severity_by_market[market_id] = candidate_status
        return severity_by_market

    @staticmethod
    def _severity_to_status(severity: str) -> str:
        if severity == "high":
            return "critical"
        if severity == "med":
            return "elevated"
        if severity == "low":
            return "watch"
        return "normal"

    @staticmethod
    def _watch_status(*, risk_status: str, active_alert_count: int) -> str:
        if active_alert_count > 0:
            return "alerting"
        if risk_status != "normal":
            return "watch"
        return "normal"

    def _build_market_health_rows(
        self,
        *,
        business_date: date,
        risk_result: RiskComputationResult | None,
    ) -> list[dict[str, object]]:
        risk_map = {row.market_id: row for row in risk_result.market_rows} if risk_result else {}
        risk_status_by_market = self._candidate_map(risk_result)
        alert_counts = {
            int(entity_id): count
            for entity_id, count in self.session.execute(
                select(Alert.entity_id, func.count())
                .where(Alert.entity_type == "market", Alert.status.in_(ACTIVE_ALERT_STATUSES))
                .group_by(Alert.entity_id)
            ).all()
            if str(entity_id).isdigit()
        }
        rows = self.session.scalars(
            select(MarketOverviewDaily).where(MarketOverviewDaily.business_date == business_date)
        ).all()
        output: list[dict[str, object]] = []
        for row in rows:
            risk_row = risk_map.get(row.market_id)
            if risk_row is not None and risk_row.kink_target_utilization is not None:
                distance_to_kink = risk_row.kink_target_utilization - risk_row.utilization
            else:
                distance_to_kink = None
            output.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": row.as_of_ts_utc,
                    "market_id": row.market_id,
                    "total_supply_usd": row.total_supply_usd,
                    "total_borrow_usd": row.total_borrow_usd,
                    "supply_apy": row.supply_apy,
                    "borrow_apy": row.borrow_apy,
                    "utilization": row.utilization,
                    "available_liquidity_usd": row.available_liquidity_usd,
                    "available_liquidity_ratio": (
                        risk_row.available_liquidity_ratio if risk_row is not None else None
                    ),
                    "borrow_apy_delta": risk_row.borrow_apy_delta if risk_row is not None else None,
                    "distance_to_kink": distance_to_kink,
                    "risk_status": risk_status_by_market.get(row.market_id, "normal"),
                    "active_alert_count": int(alert_counts.get(row.market_id, 0)),
                }
            )
        return output

    def _replace_market_health_rows(
        self, *, business_date: date, rows: list[dict[str, object]]
    ) -> None:
        self.session.execute(
            delete(MarketHealthDaily).where(MarketHealthDaily.business_date == business_date)
        )
        if rows:
            self.session.execute(insert(MarketHealthDaily).values(rows))

    def _build_exposure_rows(self, *, business_date: date) -> list[dict[str, object]]:
        as_of_ts_utc = self.session.scalar(
            select(func.max(MarketHealthDaily.as_of_ts_utc)).where(
                MarketHealthDaily.business_date == business_date
            )
        )
        health_rows = self.session.execute(
            select(
                MarketExposureComponent.market_exposure_id,
                MarketExposureComponent.component_role,
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
            .join(
                MarketExposureComponent,
                MarketExposureComponent.market_id == MarketHealthDaily.market_id,
            )
            .where(MarketHealthDaily.business_date == business_date)
        ).all()
        exposure_rows = self.session.execute(
            select(
                MarketExposure.market_exposure_id,
                MarketExposure.exposure_slug,
                Token.symbol,
            ).outerjoin(Token, Token.token_id == MarketExposure.supply_token_id)
        ).all()
        usage_by_slug = build_market_exposure_usage_metrics(
            self.session,
            as_of_ts_utc=as_of_ts_utc,
        )
        usage_map = {
            int(market_exposure_id): usage_by_slug.get(
                str(exposure_slug),
                (False, 0, ZERO, None),
            )
            for market_exposure_id, exposure_slug, _supply_symbol in exposure_rows
        }
        supply_symbol_by_exposure = {
            int(market_exposure_id): str(supply_symbol) if supply_symbol is not None else None
            for market_exposure_id, _exposure_slug, supply_symbol in exposure_rows
        }

        grouped: dict[int, list] = {}
        for row in health_rows:
            grouped.setdefault(int(row.market_exposure_id), []).append(row)

        output: list[dict[str, object]] = []
        for exposure_id, rows in grouped.items():
            primary_rows = [row for row in rows if row.component_role == "primary_market"]
            supply_rows = [
                row for row in rows if row.component_role in {"primary_market", "supply_market"}
            ]
            borrow_rows = [
                row for row in rows if row.component_role in {"primary_market", "borrow_market"}
            ]
            liquidity_rows = borrow_rows or primary_rows

            total_supply_usd = sum((row.total_supply_usd for row in supply_rows), ZERO)
            total_borrow_usd = sum((row.total_borrow_usd for row in borrow_rows), ZERO)
            total_available_liquidity_usd = sum(
                (row.available_liquidity_usd for row in liquidity_rows),
                ZERO,
            )
            weighted_supply_apy = (
                sum((row.supply_apy * row.total_supply_usd for row in supply_rows), ZERO)
                / total_supply_usd
                if total_supply_usd > ZERO
                else ZERO
            )
            weighted_borrow_apy = (
                sum((row.borrow_apy * row.total_borrow_usd for row in borrow_rows), ZERO)
                / total_borrow_usd
                if total_borrow_usd > ZERO
                else ZERO
            )
            if borrow_rows and not primary_rows:
                utilization = (
                    sum((row.utilization * row.total_borrow_usd for row in borrow_rows), ZERO)
                    / total_borrow_usd
                    if total_borrow_usd > ZERO
                    else ZERO
                )
            else:
                utilization = (
                    total_borrow_usd / total_supply_usd if total_supply_usd > ZERO else ZERO
                )
            distance_values = [
                row.distance_to_kink for row in liquidity_rows if row.distance_to_kink is not None
            ]
            distance_to_kink = min(distance_values) if distance_values else None
            active_alert_count = sum((int(row.active_alert_count) for row in rows), 0)
            risk_status = max(
                (str(row.risk_status) for row in rows), key=lambda value: SEVERITY_RANK[value]
            )
            monitored, strategy_position_count, _borrow_usd, collateral_yield_apy = usage_map.get(
                exposure_id,
                (False, 0, ZERO, None),
            )
            if collateral_yield_apy is not None:
                weighted_supply_apy = collateral_yield_apy
            elif strategy_position_count == 0:
                weighted_supply_apy = self._fallback_monitored_collateral_yield(
                    supply_symbol_by_exposure.get(exposure_id),
                    weighted_supply_apy,
                )
            customer_position_count = 1 if monitored else 0
            scope_segment = "strategy_only"
            if monitored and strategy_position_count:
                scope_segment = "overlap"
            elif monitored and not strategy_position_count:
                scope_segment = "customer_only"
            watch_status = self._watch_status(
                risk_status=risk_status,
                active_alert_count=active_alert_count,
            )
            output.append(
                {
                    "business_date": business_date,
                    "market_exposure_id": exposure_id,
                    "scope_segment": scope_segment,
                    "total_supply_usd": total_supply_usd,
                    "total_borrow_usd": total_borrow_usd,
                    "weighted_supply_apy": weighted_supply_apy,
                    "weighted_borrow_apy": weighted_borrow_apy,
                    "utilization": utilization,
                    "available_liquidity_usd": total_available_liquidity_usd,
                    "distance_to_kink": distance_to_kink,
                    "strategy_position_count": strategy_position_count,
                    "customer_position_count": customer_position_count,
                    "active_alert_count": active_alert_count,
                    "risk_status": risk_status,
                    "watch_status": watch_status,
                }
            )
        return output

    def _fallback_monitored_collateral_yield(
        self,
        supply_symbol: str | None,
        current_weighted_supply_apy: Decimal,
    ) -> Decimal:
        if supply_symbol is None or self.avant_yield_oracle is None:
            return current_weighted_supply_apy
        normalized = supply_symbol.strip().upper()
        if normalized not in AVANT_APY_ENDPOINTS:
            return current_weighted_supply_apy
        try:
            return self.avant_yield_oracle.get_token_apy(supply_symbol)
        except Exception:
            return current_weighted_supply_apy

    def _replace_exposure_rows(self, *, business_date: date, rows: list[dict[str, object]]) -> None:
        self.session.execute(
            delete(MarketExposureDaily).where(MarketExposureDaily.business_date == business_date)
        )
        if rows:
            self.session.execute(insert(MarketExposureDaily).values(rows))

    def _build_summary_rows(
        self, *, business_date: date, rows: list[dict[str, object]]
    ) -> list[dict[str, object]]:
        if not rows:
            return []
        scope_segments = {str(row["scope_segment"]) for row in rows}
        component_rows = self.session.execute(
            select(
                MarketExposureDaily.scope_segment,
                MarketHealthDaily.market_id,
                MarketHealthDaily.total_supply_usd,
                MarketHealthDaily.total_borrow_usd,
                MarketHealthDaily.available_liquidity_usd,
                MarketHealthDaily.risk_status,
                MarketHealthDaily.active_alert_count,
            )
            .join(
                MarketExposureComponent,
                (
                    MarketExposureComponent.market_exposure_id
                    == MarketExposureDaily.market_exposure_id
                ),
            )
            .join(
                MarketHealthDaily,
                (MarketHealthDaily.market_id == MarketExposureComponent.market_id)
                & (MarketHealthDaily.business_date == business_date),
            )
            .where(MarketExposureDaily.business_date == business_date)
        ).all()

        grouped: dict[str, dict[int, Any]] = {}
        for row in component_rows:
            scope_segment = str(row.scope_segment)
            if scope_segment not in scope_segments:
                continue
            grouped.setdefault(scope_segment, {})[int(row.market_id)] = row

        summaries: list[dict[str, object]] = []
        for scope_segment in sorted(scope_segments):
            scope_rows = list(grouped.get(scope_segment, {}).values())
            if not scope_rows:
                continue
            total_supply_usd = sum((Decimal(str(row.total_supply_usd)) for row in scope_rows), ZERO)
            total_borrow_usd = sum((Decimal(str(row.total_borrow_usd)) for row in scope_rows), ZERO)
            total_available_liquidity_usd = sum(
                (Decimal(str(row.available_liquidity_usd)) for row in scope_rows), ZERO
            )
            weighted_utilization = (
                total_borrow_usd / total_supply_usd if total_supply_usd > ZERO else None
            )
            summaries.append(
                {
                    "business_date": business_date,
                    "scope_segment": scope_segment,
                    "total_supply_usd": total_supply_usd,
                    "total_borrow_usd": total_borrow_usd,
                    "weighted_utilization": weighted_utilization,
                    "total_available_liquidity_usd": total_available_liquidity_usd,
                    "markets_at_risk_count": sum(
                        1 for row in scope_rows if row.risk_status in {"elevated", "critical"}
                    ),
                    "markets_on_watchlist_count": sum(
                        1
                        for row in scope_rows
                        if self._watch_status(
                            risk_status=str(row.risk_status),
                            active_alert_count=int(row.active_alert_count),
                        )
                        != "normal"
                    ),
                }
            )
        return summaries

    def _replace_summary_rows(self, *, business_date: date, rows: list[dict[str, object]]) -> None:
        self.session.execute(
            delete(MarketSummaryDaily).where(MarketSummaryDaily.business_date == business_date)
        )
        if rows:
            self.session.execute(insert(MarketSummaryDaily).values(rows))
