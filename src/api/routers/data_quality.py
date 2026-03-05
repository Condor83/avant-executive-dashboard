"""GET /data-quality — freshness, coverage, and recent issues."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.deps import get_session
from api.schemas.data_quality import (
    Coverage,
    DataQualityResponse,
    DqIssueRow,
    Freshness,
)
from core.db.models import (
    DataQuality,
    Market,
    MarketSnapshot,
    PositionSnapshot,
    Wallet,
)

router = APIRouter()


@router.get("/data-quality")
def get_data_quality(session: Session = Depends(get_session)) -> DataQualityResponse:
    now = datetime.now(UTC)

    # Freshness
    latest_ps_ts = session.scalar(select(func.max(PositionSnapshot.as_of_ts_utc)))
    latest_ms_ts = session.scalar(select(func.max(MarketSnapshot.as_of_ts_utc)))
    pos_age = (now - latest_ps_ts).total_seconds() / 3600 if latest_ps_ts else None
    mkt_age = (now - latest_ms_ts).total_seconds() / 3600 if latest_ms_ts else None

    # Coverage
    markets_configured = session.scalar(select(func.count()).select_from(Market)) or 0
    wallets_configured = (
        session.scalar(
            select(func.count()).select_from(Wallet).where(Wallet.wallet_type == "strategy")
        )
        or 0
    )

    if latest_ms_ts is not None:
        markets_with_snapshots = (
            session.scalar(
                select(func.count(func.distinct(MarketSnapshot.market_id))).where(
                    MarketSnapshot.as_of_ts_utc == latest_ms_ts
                )
            )
            or 0
        )
    else:
        markets_with_snapshots = 0

    if latest_ps_ts is not None:
        wallets_with_positions = (
            session.scalar(
                select(func.count(func.distinct(PositionSnapshot.wallet_id))).where(
                    PositionSnapshot.as_of_ts_utc == latest_ps_ts
                )
            )
            or 0
        )
    else:
        wallets_with_positions = 0

    # Recent issues
    recent = session.scalars(
        select(DataQuality).order_by(DataQuality.as_of_ts_utc.desc()).limit(50)
    ).all()

    issue_count_24h = (
        session.scalar(
            select(func.count())
            .select_from(DataQuality)
            .where(DataQuality.created_at >= now - timedelta(hours=24))
        )
        or 0
    )

    return DataQualityResponse(
        freshness=Freshness(
            last_position_snapshot_utc=latest_ps_ts,
            last_market_snapshot_utc=latest_ms_ts,
            position_snapshot_age_hours=round(pos_age, 2) if pos_age is not None else None,
            market_snapshot_age_hours=round(mkt_age, 2) if mkt_age is not None else None,
        ),
        coverage=Coverage(
            markets_with_snapshots=markets_with_snapshots,
            markets_configured=markets_configured,
            wallets_with_positions=wallets_with_positions,
            wallets_configured=wallets_configured,
        ),
        recent_issues=[
            DqIssueRow(
                data_quality_id=r.data_quality_id,
                as_of_ts_utc=r.as_of_ts_utc,
                stage=r.stage,
                protocol_code=r.protocol_code,
                chain_code=r.chain_code,
                error_type=r.error_type,
                error_message=r.error_message,
            )
            for r in recent
        ],
        issue_count_24h=issue_count_24h,
    )
