"""Alert candidate generation and lifecycle synchronization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from analytics.risk_engine import RiskComputationResult
from core.config import DecreasingRiskThresholds, IncreasingRiskThresholds, RiskThresholdsConfig
from core.db.models import Alert

SEVERITY_RANK = {"low": 1, "med": 2, "high": 3}
ACTIVE_ALERT_STATUSES = ("open", "ack")


@dataclass(frozen=True)
class AlertCandidate:
    """Threshold breach translated to a canonical alert shape."""

    alert_type: str
    severity: str
    entity_type: str
    entity_id: str
    payload_json: dict[str, Any]

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.alert_type, self.entity_type, self.entity_id)


@dataclass(frozen=True)
class AlertSyncSummary:
    """Lifecycle sync counts for a compute risk run."""

    opened: int
    updated: int
    resolved: int
    open_alerts: int


def classify_increasing_risk(
    value: Decimal | None,
    *,
    thresholds: IncreasingRiskThresholds,
) -> str | None:
    """Classify severity when higher values are riskier."""

    if value is None:
        return None
    if value >= thresholds.high:
        return "high"
    if value >= thresholds.med:
        return "med"
    if value >= thresholds.low:
        return "low"
    return None


def classify_decreasing_risk(
    value: Decimal | None,
    *,
    thresholds: DecreasingRiskThresholds,
) -> str | None:
    """Classify severity when lower values are riskier."""

    if value is None:
        return None
    if value <= thresholds.high:
        return "high"
    if value <= thresholds.med:
        return "med"
    if value <= thresholds.low:
        return "low"
    return None


def list_open_alerts(session: Session, *, limit: int = 200) -> list[Alert]:
    """Fetch currently open alerts for API/dashboard consumers."""

    return list(
        session.scalars(
            select(Alert)
            .where(Alert.status == "open")
            .order_by(Alert.ts_utc.desc(), Alert.alert_id.desc())
            .limit(limit)
        )
    )


class AlertEngine:
    """Generate and persist alerts from risk scoring outputs."""

    def __init__(self, session: Session, *, thresholds: RiskThresholdsConfig) -> None:
        self.session = session
        self.thresholds = thresholds

    def build_candidates(self, risk_result: RiskComputationResult) -> list[AlertCandidate]:
        """Build alert candidates for all breached risk signals."""

        candidates: list[AlertCandidate] = []

        for market_row in risk_result.market_rows:
            kink_severity = classify_increasing_risk(
                market_row.utilization,
                thresholds=self.thresholds.kink.utilization,
            )
            if kink_severity is not None:
                candidates.append(
                    AlertCandidate(
                        alert_type="KINK_NEAR",
                        severity=kink_severity,
                        entity_type="market",
                        entity_id=str(market_row.market_id),
                        payload_json={
                            "as_of_ts_utc": risk_result.as_of_ts_utc.isoformat(),
                            "protocol_code": market_row.protocol_code,
                            "chain_code": market_row.chain_code,
                            "market_address": market_row.market_address,
                            "market_id": market_row.market_id,
                            "utilization": str(market_row.utilization),
                            "kink_target_utilization": str(market_row.kink_target_utilization),
                            "kink_score": str(market_row.kink_score),
                        },
                    )
                )

            borrow_spike_severity = classify_increasing_risk(
                market_row.borrow_apy_delta,
                thresholds=self.thresholds.borrow_spike.delta_apy,
            )
            if borrow_spike_severity is not None:
                candidates.append(
                    AlertCandidate(
                        alert_type="BORROW_RATE_SPIKE",
                        severity=borrow_spike_severity,
                        entity_type="market",
                        entity_id=str(market_row.market_id),
                        payload_json={
                            "as_of_ts_utc": risk_result.as_of_ts_utc.isoformat(),
                            "protocol_code": market_row.protocol_code,
                            "chain_code": market_row.chain_code,
                            "market_address": market_row.market_address,
                            "market_id": market_row.market_id,
                            "borrow_apy": str(market_row.borrow_apy),
                            "borrow_apy_delta": (
                                str(market_row.borrow_apy_delta)
                                if market_row.borrow_apy_delta is not None
                                else None
                            ),
                            "max_lookback_hours": self.thresholds.borrow_spike.max_lookback_hours,
                        },
                    )
                )

            liquidity_severity = classify_decreasing_risk(
                market_row.available_liquidity_ratio,
                thresholds=self.thresholds.liquidity.available_ratio,
            )
            if liquidity_severity is not None:
                candidates.append(
                    AlertCandidate(
                        alert_type="LIQUIDITY_SQUEEZE",
                        severity=liquidity_severity,
                        entity_type="market",
                        entity_id=str(market_row.market_id),
                        payload_json={
                            "as_of_ts_utc": risk_result.as_of_ts_utc.isoformat(),
                            "protocol_code": market_row.protocol_code,
                            "chain_code": market_row.chain_code,
                            "market_address": market_row.market_address,
                            "market_id": market_row.market_id,
                            "available_liquidity_usd": str(market_row.available_liquidity_usd),
                            "total_supply_usd": str(market_row.total_supply_usd),
                            "available_liquidity_ratio": str(market_row.available_liquidity_ratio),
                        },
                    )
                )

        for position_row in risk_result.position_rows:
            spread_severity = classify_decreasing_risk(
                position_row.net_spread_apy,
                thresholds=self.thresholds.spread.net_spread_apy,
            )
            if spread_severity is not None:
                candidates.append(
                    AlertCandidate(
                        alert_type="SPREAD_TOO_TIGHT",
                        severity=spread_severity,
                        entity_type="position",
                        entity_id=position_row.position_key,
                        payload_json={
                            "as_of_ts_utc": risk_result.as_of_ts_utc.isoformat(),
                            "position_key": position_row.position_key,
                            "wallet_id": position_row.wallet_id,
                            "market_id": position_row.market_id,
                            "supply_apy": str(position_row.supply_apy),
                            "reward_apy": str(position_row.reward_apy),
                            "borrow_apy": str(position_row.borrow_apy),
                            "net_spread_apy": str(position_row.net_spread_apy),
                        },
                    )
                )

        deduped: dict[tuple[str, str, str], AlertCandidate] = {}
        for candidate in candidates:
            current = deduped.get(candidate.key)
            if current is None:
                deduped[candidate.key] = candidate
                continue
            if SEVERITY_RANK[candidate.severity] > SEVERITY_RANK[current.severity]:
                deduped[candidate.key] = candidate

        return list(deduped.values())

    def sync_candidates(
        self,
        *,
        as_of_ts_utc: datetime,
        candidates: list[AlertCandidate],
    ) -> AlertSyncSummary:
        """Upsert active alerts for current breaches and resolve cleared breaches."""

        desired_by_key = {candidate.key: candidate for candidate in candidates}

        existing_active_alerts = list(
            self.session.scalars(select(Alert).where(Alert.status.in_(ACTIVE_ALERT_STATUSES)))
        )
        existing_by_key = {
            (alert.alert_type, alert.entity_type, alert.entity_id): alert
            for alert in existing_active_alerts
        }

        opened = 0
        updated = 0

        for key, candidate in desired_by_key.items():
            existing = existing_by_key.pop(key, None)
            if existing is None:
                self.session.add(
                    Alert(
                        ts_utc=as_of_ts_utc,
                        alert_type=candidate.alert_type,
                        severity=candidate.severity,
                        entity_type=candidate.entity_type,
                        entity_id=candidate.entity_id,
                        payload_json=candidate.payload_json,
                        status="open",
                    )
                )
                opened += 1
                continue

            existing.ts_utc = as_of_ts_utc
            existing.severity = candidate.severity
            existing.payload_json = candidate.payload_json
            updated += 1

        resolved = 0
        for stale_alert in existing_by_key.values():
            stale_alert.ts_utc = as_of_ts_utc
            stale_alert.status = "resolved"
            resolved += 1

        open_alerts = int(
            self.session.scalar(select(func.count(Alert.alert_id)).where(Alert.status == "open"))
            or 0
        )

        return AlertSyncSummary(
            opened=opened,
            updated=updated,
            resolved=resolved,
            open_alerts=open_alerts,
        )
