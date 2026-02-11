"""add index kline source record table

Revision ID: 0004_index_kline_source_record
Revises: 0003_index_quote_tables
Create Date: 2026-02-11

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0004_index_kline_source_record"
down_revision = "0003_index_quote_tables"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    return sa.inspect(op.get_bind()).has_table(table_name)


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE klineinterval AS ENUM ('1m', '5m');
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    if not _has_table("index_kline_source_record"):
        op.create_table(
            "index_kline_source_record",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("index_id", sa.Integer(), sa.ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False),
            sa.Column(
                "interval",
                postgresql.ENUM("1m", "5m", name="klineinterval", create_type=False),
                nullable=False,
            ),
            sa.Column("bar_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("source", sa.String(length=32), nullable=False),
            sa.Column("open", sa.Integer(), nullable=True),
            sa.Column("high", sa.Integer(), nullable=True),
            sa.Column("low", sa.Integer(), nullable=True),
            sa.Column("close", sa.Integer(), nullable=True),
            sa.Column("volume", sa.BigInteger(), nullable=True),
            sa.Column("turnover_amount", sa.BigInteger(), nullable=True),
            sa.Column("turnover_currency", sa.String(length=8), nullable=True),
            sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=True),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("error", sa.Text(), nullable=True),
            sa.UniqueConstraint("index_id", "interval", "bar_time", "source", name="uq_index_kline_source"),
        )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_kline_lookup "
        "ON index_kline_source_record (index_id, interval, bar_time DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_kline_trade_date "
        "ON index_kline_source_record (trade_date, interval)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_index_kline_source_fetched "
        "ON index_kline_source_record (source, fetched_at DESC)"
    )


def downgrade() -> None:
    op.drop_index("ix_index_kline_source_fetched", table_name="index_kline_source_record")
    op.drop_index("ix_index_kline_trade_date", table_name="index_kline_source_record")
    op.drop_index("ix_index_kline_lookup", table_name="index_kline_source_record")
    op.drop_table("index_kline_source_record")
    op.execute("DROP TYPE IF EXISTS klineinterval")

