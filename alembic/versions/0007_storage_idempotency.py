"""add storage-level idempotency for yield rows and active alerts

Revision ID: 0007_storage_idempotency
Revises: 0006_market_overview
Create Date: 2026-03-05 11:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0007_storage_idempotency"
down_revision = "0006_market_overview"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("yield_daily", sa.Column("row_key", sa.String(length=255), nullable=True))
    op.execute(
        """
        UPDATE yield_daily
        SET row_key = CASE
            WHEN position_key IS NOT NULL THEN 'position:' || position_key
            WHEN wallet_id IS NOT NULL THEN 'wallet:' || wallet_id::text
            WHEN product_id IS NOT NULL THEN 'product:' || product_id::text
            WHEN protocol_id IS NOT NULL THEN 'protocol:' || protocol_id::text
            ELSE 'total'
        END
        WHERE row_key IS NULL
        """
    )
    op.execute(
        """
        DELETE FROM yield_daily
        WHERE yield_daily_id IN (
            SELECT yield_daily_id
            FROM (
                SELECT yield_daily_id,
                       row_number() OVER (
                           PARTITION BY business_date, method, row_key
                           ORDER BY yield_daily_id DESC
                       ) AS row_num
                FROM yield_daily
            ) ranked
            WHERE ranked.row_num > 1
        )
        """
    )
    op.alter_column("yield_daily", "row_key", existing_type=sa.String(length=255), nullable=False)
    op.create_unique_constraint(
        "uq_yield_daily_business_date_method_row_key",
        "yield_daily",
        ["business_date", "method", "row_key"],
    )

    op.execute(
        """
        WITH ranked AS (
            SELECT alert_id,
                   row_number() OVER (
                       PARTITION BY alert_type, entity_type, entity_id
                       ORDER BY CASE WHEN status = 'ack' THEN 0 ELSE 1 END,
                                ts_utc DESC,
                                alert_id DESC
                   ) AS row_num
            FROM alerts
            WHERE status IN ('open', 'ack')
        )
        UPDATE alerts
        SET status = 'resolved'
        WHERE alert_id IN (
            SELECT alert_id
            FROM ranked
            WHERE row_num > 1
        )
        """
    )
    op.create_index(
        "uq_alerts_active_key",
        "alerts",
        ["alert_type", "entity_type", "entity_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('open', 'ack')"),
    )


def downgrade() -> None:
    op.drop_index("uq_alerts_active_key", table_name="alerts")
    op.drop_constraint(
        "uq_yield_daily_business_date_method_row_key",
        "yield_daily",
        type_="unique",
    )
    op.drop_column("yield_daily", "row_key")
