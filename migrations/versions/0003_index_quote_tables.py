"""add index quote tables

Revision ID: 0003_index_quote_tables
Revises: 0002_hsi_quote_fact
Create Date: 2026-02-11

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0003_index_quote_tables"
down_revision = "0002_hsi_quote_fact"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    if not _has_table("market_index"):
        op.create_table(
            "market_index",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("code", sa.String(length=16), nullable=False),
            sa.Column("name_zh", sa.String(length=64), nullable=False),
            sa.Column("name_en", sa.String(length=64), nullable=True),
            sa.Column("market", sa.String(length=16), nullable=False),
            sa.Column("exchange", sa.String(length=32), nullable=False),
            sa.Column("currency", sa.String(length=8), nullable=False, server_default=sa.text("'HKD'")),
            sa.Column("timezone", sa.String(length=64), nullable=False, server_default=sa.text("'Asia/Shanghai'")),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("display_order", sa.Integer(), nullable=False, server_default=sa.text("100")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("code", name="uq_market_index_code"),
        )

    op.execute(
        """
        INSERT INTO market_index (code, name_zh, name_en, market, exchange, currency, timezone, display_order)
        VALUES
            ('HSI', '恒生指数', 'Hang Seng Index', 'HK', 'HKEX', 'HKD', 'Asia/Hong_Kong', 10),
            ('SSE', '上证指数', 'SSE Composite Index', 'CN', 'SSE', 'CNY', 'Asia/Shanghai', 20),
            ('SZSE', '深证成指', 'SZSE Component Index', 'CN', 'SZSE', 'CNY', 'Asia/Shanghai', 30)
        ON CONFLICT (code) DO NOTHING
        """
    )

    if not _has_table("index_quote_source_record"):
        op.create_table(
            "index_quote_source_record",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
            sa.Column("source", sa.String(length=32), nullable=False),
            sa.Column("last", sa.Integer(), nullable=True),
            sa.Column("change_points", sa.Integer(), nullable=True),
            sa.Column("change_pct", sa.Integer(), nullable=True),
            sa.Column("turnover_amount", sa.BigInteger(), nullable=True),
            sa.Column("turnover_currency", sa.String(length=8), nullable=True),
            sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=True),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("error", sa.Text(), nullable=True),
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_source_record_lookup "
        "ON index_quote_source_record (index_id, trade_date, session)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_source_record_source_fetched "
        "ON index_quote_source_record (source, fetched_at DESC)"
    )

    if not _has_table("index_quote_history"):
        op.create_table(
            "index_quote_history",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
            sa.Column("last", sa.Integer(), nullable=False),
            sa.Column("change_points", sa.Integer(), nullable=True),
            sa.Column("change_pct", sa.Integer(), nullable=True),
            sa.Column("turnover_amount", sa.BigInteger(), nullable=True),
            sa.Column("turnover_currency", sa.String(length=8), nullable=False, server_default=sa.text("'HKD'")),
            sa.Column("best_source", sa.String(length=32), nullable=False),
            sa.Column("quality", postgresql.ENUM("official", "provisional", "estimated", "fallback", name="quality", create_type=False), nullable=False),
            sa.Column("source_count", sa.Integer(), nullable=False, server_default=sa.text("1")),
            sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=True),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("index_id", "trade_date", "session", name="uq_index_quote_history"),
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_quote_history_index_date "
        "ON index_quote_history (index_id, trade_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_quote_history_trade_session "
        "ON index_quote_history (trade_date, session)"
    )

    if not _has_table("index_realtime_snapshot"):
        op.create_table(
            "index_realtime_snapshot",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("session", postgresql.ENUM("AM", "FULL", name="sessiontype", create_type=False), nullable=False),
            sa.Column("last", sa.Integer(), nullable=False),
            sa.Column("change_points", sa.Integer(), nullable=True),
            sa.Column("change_pct", sa.Integer(), nullable=True),
            sa.Column("turnover_amount", sa.BigInteger(), nullable=True),
            sa.Column("turnover_currency", sa.String(length=8), nullable=False, server_default=sa.text("'HKD'")),
            sa.Column("data_updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("source", sa.String(length=32), nullable=False),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.UniqueConstraint("index_id", "trade_date", name="uq_index_realtime_snapshot"),
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_realtime_snapshot_date "
        "ON index_realtime_snapshot (trade_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_realtime_snapshot_index "
        "ON index_realtime_snapshot (index_id)"
    )


def downgrade() -> None:
    op.drop_index("ix_index_realtime_snapshot_index", table_name="index_realtime_snapshot")
    op.drop_index("ix_index_realtime_snapshot_date", table_name="index_realtime_snapshot")
    op.drop_table("index_realtime_snapshot")

    op.drop_index("ix_index_quote_history_trade_session", table_name="index_quote_history")
    op.drop_index("ix_index_quote_history_index_date", table_name="index_quote_history")
    op.drop_table("index_quote_history")

    op.drop_index("ix_index_source_record_source_fetched", table_name="index_quote_source_record")
    op.drop_index("ix_index_source_record_lookup", table_name="index_quote_source_record")
    op.drop_table("index_quote_source_record")

    op.drop_table("market_index")
