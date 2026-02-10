"""init

Revision ID: 0001_init
Revises: 
Create Date: 2026-02-10

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trading_calendar_hk",
        sa.Column("trade_date", sa.Date(), primary_key=True),
        sa.Column("is_trading_day", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_half_day", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notes", sa.Text(), nullable=True),
    )

    # Be idempotent even if the container was restarted mid-migration
    op.execute("""
    DO $$ BEGIN
        CREATE TYPE sessiontype AS ENUM ('AM','FULL');
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END $$;
    """)

    op.execute("""
    DO $$ BEGIN
        CREATE TYPE quality AS ENUM ('official','provisional','estimated','fallback');
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END $$;
    """)

    op.create_table(
        "turnover_source_record",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("turnover_hkd", sa.BigInteger(), nullable=True),
        sa.Column("cutoff_time", sa.Time(), nullable=True),
        sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_source_record_trade_session",
        "turnover_source_record",
        ["trade_date", "session"],
    )
    op.create_index(
        "ix_source_record_source_fetched",
        "turnover_source_record",
        ["source", sa.text("fetched_at DESC")],
    )

    op.create_table(
        "turnover_fact",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
        sa.Column("turnover_hkd", sa.BigInteger(), nullable=False),
        sa.Column("cutoff_time", sa.Time(), nullable=True),
        sa.Column("is_half_day_market", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("best_source", sa.String(length=32), nullable=False),
        sa.Column("quality", postgresql.ENUM("official", "provisional", "estimated", "fallback", name="quality", create_type=False), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("trade_date", "session", name="uq_fact_trade_session"),
    )
    op.create_index(
        "ix_fact_session_trade_desc",
        "turnover_fact",
        ["session", sa.text("trade_date DESC")],
    )

    op.create_table(
        "job_run",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("job_name", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'running'")),
        sa.Column("summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("job_run")
    op.drop_index("ix_fact_session_trade_desc", table_name="turnover_fact")
    op.drop_table("turnover_fact")
    op.drop_index("ix_source_record_source_fetched", table_name="turnover_source_record")
    op.drop_index("ix_source_record_trade_session", table_name="turnover_source_record")
    op.drop_table("turnover_source_record")
    op.drop_table("trading_calendar_hk")

    op.execute("DROP TYPE IF EXISTS quality")
    op.execute("DROP TYPE IF EXISTS sessiontype")
