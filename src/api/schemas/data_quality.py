"""Response schema for GET /data-quality."""

from datetime import datetime

from pydantic import BaseModel


class Freshness(BaseModel):
    last_position_snapshot_utc: datetime | None
    last_market_snapshot_utc: datetime | None
    position_snapshot_age_hours: float | None
    market_snapshot_age_hours: float | None


class Coverage(BaseModel):
    markets_with_snapshots: int
    markets_configured: int
    wallets_with_positions: int
    wallets_configured: int


class DqIssueRow(BaseModel):
    data_quality_id: int
    as_of_ts_utc: datetime
    stage: str
    protocol_code: str | None
    chain_code: str | None
    error_type: str
    error_message: str


class DataQualityResponse(BaseModel):
    freshness: Freshness
    coverage: Coverage
    recent_issues: list[DqIssueRow]
    issue_count_24h: int
