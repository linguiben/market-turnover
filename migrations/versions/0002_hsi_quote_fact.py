"""add hsi_quote_fact

Revision ID: 0002_hsi_quote_fact
Revises: 0001_init
Create Date: 2026-02-10

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0002_hsi_quote_fact"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "hsi_quote_fact",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
        sa.Column("last", sa.Integer(), nullable=False),
        sa.Column("change", sa.Integer(), nullable=True),
        sa.Column("change_pct", sa.Integer(), nullable=True),
        sa.Column("turnover_hkd", sa.BigInteger(), nullable=True),
        sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default=sa.text("'AASTOCKS'")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("trade_date", "session", name="uq_hsi_trade_session"),
    )
    op.create_index(
        "ix_hsi_trade_desc",
        "hsi_quote_fact",
        ["session", sa.text("trade_date DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_hsi_trade_desc", table_name="hsi_quote_fact")
    op.drop_table("hsi_quote_fact")
