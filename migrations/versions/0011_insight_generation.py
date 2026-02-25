"""add insight prompt/snapshot tables

Revision ID: 0011_insight_generation
Revises: 0010_index_realtime_api_snapshot
Create Date: 2026-02-25

"""

from __future__ import annotations

from pathlib import Path

from alembic import op


revision = "0011_insight_generation"
down_revision = "0010_index_realtime_api_snapshot"
branch_labels = None
depends_on = None


def _execute_sql_file(filename: str) -> None:
    base = Path(__file__).resolve().parents[1] / "sql"
    sql_text = (base / filename).read_text(encoding="utf-8")
    for statement in sql_text.split(";"):
        stmt = statement.strip()
        if not stmt:
            continue
        op.execute(stmt)


def upgrade() -> None:
    _execute_sql_file("0011_insight_generation.sql")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS insight_snapshot")
    op.execute("DROP TABLE IF EXISTS insight_sys_prompt")
