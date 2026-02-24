"""add global markets indices to market_index

Revision ID: 0009_add_global_markets_indices
Revises: 0008_app_cache
Create Date: 2026-02-24

"""

from __future__ import annotations

from alembic import op


revision = "0009_add_global_markets_indices"
down_revision = "0008_app_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add new global markets indices to market_index table
    op.execute(
        """
        INSERT INTO market_index (code, name_zh, name_en, market, exchange, currency, timezone, display_order)
        VALUES
            ('DJI', '道琼斯指数', 'Dow Jones', 'US', 'NYSE', 'USD', 'America/New_York', 40),
            ('IXIC', '纳斯达克', 'NASDAQ', 'US', 'NASDAQ', 'USD', 'America/New_York', 50),
            ('SPX', '标普500', 'S&P 500', 'US', 'NYSE', 'USD', 'America/New_York', 55),
            ('N225', '日经225', 'Nikkei 225', 'JP', 'TSE', 'JPY', 'Asia/Tokyo', 60),
            ('FTSE', '富时100', 'FTSE 100', 'UK', 'LSE', 'GBP', 'Europe/London', 70),
            ('GDAXI', '德国DAX', 'DAX', 'DE', 'Xetra', 'EUR', 'Europe/Berlin', 80),
            ('CSX5P', '欧洲斯托克50', 'Euro Stoxx 50', 'EU', 'EUREX', 'EUR', 'Europe/Berlin', 90),
            ('KS11', '韩国综合指数', 'KOSPI', 'KR', 'KRX', 'KRW', 'Asia/Seoul', 35)
        ON CONFLICT (code) DO NOTHING
        """
    )


def downgrade() -> None:
    # Remove the added indices (will fail if data exists)
    op.execute(
        """
        DELETE FROM market_index 
        WHERE code IN ('DJI', 'IXIC', 'SPX', 'N225', 'FTSE', 'GDAXI', 'CSX5P', 'KS11')
        """
    )
