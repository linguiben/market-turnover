"""add index_realtime_api_snapshot table

Revision ID: 0010_index_realtime_api_snapshot
Revises: 0009_add_global_markets_indices
Create Date: 2026-02-24

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0010_index_realtime_api_snapshot"
down_revision = "0009_add_global_markets_indices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "index_realtime_api_snapshot",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("secid", sa.String(length=32), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column(
            "session",
            postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False),
            nullable=False,
        ),
        sa.Column("last", sa.Integer(), nullable=True),
        sa.Column("change_points", sa.Integer(), nullable=True),
        sa.Column("change_pct", sa.Integer(), nullable=True),
        sa.Column("turnover_amount", sa.BigInteger(), nullable=True),
        sa.Column("turnover_currency", sa.String(length=8), nullable=False, server_default="HKD"),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("data_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="EASTMONEY_STOCK_GET"),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index("ix_index_realtime_api_snapshot_date", "index_realtime_api_snapshot", [sa.text("trade_date DESC")])
    op.create_index("ix_index_realtime_api_snapshot_index", "index_realtime_api_snapshot", ["index_id"])
    op.create_index(
        "ix_index_realtime_api_snapshot_latest",
        "index_realtime_api_snapshot",
        ["index_id", "trade_date", sa.text("id DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_index_realtime_api_snapshot_latest", table_name="index_realtime_api_snapshot")
    op.drop_index("ix_index_realtime_api_snapshot_index", table_name="index_realtime_api_snapshot")
    op.drop_index("ix_index_realtime_api_snapshot_date", table_name="index_realtime_api_snapshot")
    op.drop_table("index_realtime_api_snapshot")
