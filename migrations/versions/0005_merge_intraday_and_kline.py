"""merge heads: intraday_bar + kline_source_record

Revision ID: 0005_merge_intraday_and_kline
Revises: 0004_index_intraday_bar, 0004_index_kline_source_record
Create Date: 2026-02-11

"""

from __future__ import annotations


revision = "0005_merge_intraday_and_kline"
down_revision = ("0004_index_intraday_bar", "0004_index_kline_source_record")
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Merge revision only (no-op)
    pass


def downgrade() -> None:
    # No-op
    pass
