"""make index_realtime_snapshot append-only

Revision ID: 0006_rt_snap_append
Revises: 0005_merge_intraday_and_kline
Create Date: 2026-02-11

"""

from __future__ import annotations

from alembic import op


revision = "0006_rt_snap_append"
down_revision = "0005_merge_intraday_and_kline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop unique constraint so snapshots won't be overwritten
    op.execute("ALTER TABLE index_realtime_snapshot DROP CONSTRAINT IF EXISTS uq_index_realtime_snapshot")

    # Add an index to speed up "latest snapshot" lookup
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_realtime_snapshot_latest "
        "ON index_realtime_snapshot (index_id, trade_date, id DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_index_realtime_snapshot_latest")
    op.execute(
        "ALTER TABLE index_realtime_snapshot "
        "ADD CONSTRAINT uq_index_realtime_snapshot UNIQUE (index_id, trade_date)"
    )
