"""add index intraday bar table

Revision ID: 0004_index_intraday_bar
Revises: 0003_index_quote_tables
Create Date: 2026-02-11

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0004_index_intraday_bar"
down_revision = "0003_index_quote_tables"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if _has_table("index_intraday_bar"):
        return

    op.create_table(
        "index_intraday_bar",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
        sa.Column("interval_min", sa.Integer(), nullable=False),
        sa.Column("bar_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Integer(), nullable=True),
        sa.Column("high", sa.Integer(), nullable=True),
        sa.Column("low", sa.Integer(), nullable=True),
        sa.Column("close", sa.Integer(), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("amount", sa.BigInteger(), nullable=True),
        sa.Column("currency", sa.String(length=8), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("index_id", "interval_min", "bar_ts", "source", name="uq_index_intraday_bar"),
    )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_intraday_bar_lookup "
        "ON index_intraday_bar (index_id, interval_min, bar_ts DESC)"
    )


def downgrade() -> None:
    op.drop_index("ix_index_intraday_bar_lookup", table_name="index_intraday_bar")
    op.drop_table("index_intraday_bar")
