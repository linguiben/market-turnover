"""add app_user and user_visit_logs

Revision ID: 0007_user_visit_logs
Revises: 0006_rt_snap_append
Create Date: 2026-02-11

"""

from __future__ import annotations

from alembic import op


revision = "0007_user_visit_logs"
down_revision = "0006_rt_snap_append"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app_user (
          id BIGSERIAL PRIMARY KEY,
          username VARCHAR(320) NOT NULL,
          email VARCHAR(320) NOT NULL,
          password_hash VARCHAR(255) NOT NULL,
          display_name VARCHAR(64),
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
          last_login_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

          CONSTRAINT ck_app_user_username_eq_email CHECK (username = email),
          CONSTRAINT ck_app_user_email_lowercase CHECK (email = lower(email)),
          CONSTRAINT ck_app_user_username_lowercase CHECK (username = lower(username)),
          CONSTRAINT ck_app_user_email_format CHECK (
            email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
          ),
          CONSTRAINT uq_app_user_username UNIQUE (username),
          CONSTRAINT uq_app_user_email UNIQUE (email)
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_app_user_active ON app_user (is_active);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_app_user_created_at ON app_user (created_at DESC);")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_visit_logs (
          id BIGSERIAL PRIMARY KEY,
          user_id INT,
          ip_address INET NOT NULL,
          session_id VARCHAR(100),
          action_type VARCHAR(20),

          user_agent TEXT,
          browser_family VARCHAR(50),
          os_family VARCHAR(50),
          device_type VARCHAR(20),

          request_url TEXT NOT NULL,
          referer_url TEXT,

          request_headers JSONB,

          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 1) time range queries
    op.execute("CREATE INDEX IF NOT EXISTS idx_visit_logs_created_at ON user_visit_logs (created_at DESC);")

    # 2) per-user audit
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_visit_logs_user_id ON user_visit_logs (user_id) WHERE user_id IS NOT NULL;"
    )

    # 3) IP audit (supports inet subnet queries)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_visit_logs_ip ON user_visit_logs USING GIST (ip_address inet_ops);"
    )

    # 4) referer analysis
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_visit_logs_referer ON user_visit_logs (referer_url) WHERE referer_url IS NOT NULL;"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS user_visit_logs;")
    op.execute("DROP TABLE IF EXISTS app_user;")
