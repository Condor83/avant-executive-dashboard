"""GET /alerts — risk alert listing."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.alerts import AlertRow
from core.db.models import Alert

router = APIRouter()


@router.get("/alerts")
def get_alerts(
    severity: str | None = Query(default=None),
    status: str = Query(default="open"),
    alert_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
) -> list[AlertRow]:
    stmt = select(Alert).where(Alert.status == status)
    if severity is not None:
        stmt = stmt.where(Alert.severity == severity)
    if alert_type is not None:
        stmt = stmt.where(Alert.alert_type == alert_type)
    stmt = stmt.order_by(Alert.ts_utc.desc()).limit(limit)

    rows = session.scalars(stmt).all()
    return [
        AlertRow(
            alert_id=r.alert_id,
            ts_utc=r.ts_utc,
            alert_type=r.alert_type,
            severity=r.severity,
            entity_type=r.entity_type,
            entity_id=r.entity_id,
            payload_json=r.payload_json,
            status=r.status,
        )
        for r in rows
    ]
