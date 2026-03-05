"""Response schema for GET /alerts."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class AlertRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    alert_id: int
    ts_utc: datetime
    alert_type: str
    severity: str
    entity_type: str
    entity_id: str
    payload_json: dict | None
    status: str
