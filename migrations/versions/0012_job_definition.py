"""add job definition and schedule tables

Revision ID: 0012_job_definition
Revises: 0011_insight_generation
Create Date: 2026-02-25

"""

from __future__ import annotations

from pathlib import Path

from alembic import op


revision = "0012_job_definition"
down_revision = "0011_insight_generation"
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
    _execute_sql_file("0012_job_definition.sql")
    _execute_sql_file("0012_job_definition_seed.sql")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS job_schedule")
    op.execute("DROP TABLE IF EXISTS job_definition")
