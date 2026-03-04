"""add alerts derived analytics table

Revision ID: 0005_alerts
Revises: 0004_yield_daily_roe
Create Date: 2026-03-04 09:30:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0005_alerts"
down_revision = "0004_yield_daily_roe"
branch_labels = None
depends_on = None


ALERT_SEVERITY_ENUM = sa.Enum("low", "med", "high", name="alert_severity_enum", native_enum=False)
ALERT_ENTITY_TYPE_ENUM = sa.Enum(
    "market",
    "position",
    "wallet",
    name="alert_entity_type_enum",
    native_enum=False,
)
ALERT_STATUS_ENUM = sa.Enum("open", "ack", "resolved", name="alert_status_enum", native_enum=False)


def upgrade() -> None:
    op.create_table(
        "alerts",
        sa.Column("alert_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("alert_type", sa.String(length=64), nullable=False),
        sa.Column("severity", ALERT_SEVERITY_ENUM, nullable=False),
        sa.Column("entity_type", ALERT_ENTITY_TYPE_ENUM, nullable=False),
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", ALERT_STATUS_ENUM, nullable=False),
    )
    op.create_index("ix_alerts_ts_utc", "alerts", ["ts_utc"])
    op.create_index("ix_alerts_status", "alerts", ["status"])
    op.create_index("ix_alerts_alert_type", "alerts", ["alert_type"])
    op.create_index("ix_alerts_entity_id", "alerts", ["entity_id"])


def downgrade() -> None:
    op.drop_index("ix_alerts_entity_id", table_name="alerts")
    op.drop_index("ix_alerts_alert_type", table_name="alerts")
    op.drop_index("ix_alerts_status", table_name="alerts")
    op.drop_index("ix_alerts_ts_utc", table_name="alerts")
    op.drop_table("alerts")
