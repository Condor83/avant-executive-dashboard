"""Risk alert listing with server-owned labels."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.alerts import AlertRow
from core.dashboard_contracts import alert_severity_label, alert_status_label
from core.db.models import Alert

router = APIRouter()


def _alert_row(alert: Alert) -> AlertRow:
    return AlertRow(
        alert_id=alert.alert_id,
        ts_utc=alert.ts_utc,
        alert_type=alert.alert_type,
        alert_type_label=alert.alert_type.replace("_", " ").title(),
        severity=alert.severity,
        severity_label=alert_severity_label(alert.severity),
        entity_type=alert.entity_type,
        entity_id=alert.entity_id,
        payload_json=alert.payload_json,
        status=alert.status,
        status_label=alert_status_label(alert.status),
    )


@router.get("/alerts")
def get_alerts(
    severity: str | None = Query(default=None),
    status: str | None = Query(default="open"),
    alert_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
) -> list[AlertRow]:
    stmt = select(Alert)
    if status is not None:
        stmt = stmt.where(Alert.status == status)
    if severity is not None:
        stmt = stmt.where(Alert.severity == severity)
    if alert_type is not None:
        stmt = stmt.where(Alert.alert_type == alert_type)
    stmt = stmt.order_by(Alert.ts_utc.desc()).limit(limit)

    return [_alert_row(row) for row in session.scalars(stmt).all()]
