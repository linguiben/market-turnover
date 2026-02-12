"""add app_cache table

Revision ID: 0008_app_cache
Revises: 0007_user_visit_logs
Create Date: 2026-02-12

"""

from __future__ import annotations

from alembic import op


revision = "0008_app_cache"
down_revision = "0007_user_visit_logs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app_cache (
          key VARCHAR(128) PRIMARY KEY,
          payload JSONB,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app_cache;")
