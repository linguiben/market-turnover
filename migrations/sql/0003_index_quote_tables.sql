-- 0003_index_quote_tables.sql
-- 对应 Alembic 版本: 0003_index_quote_tables
-- 说明: 支持独立执行；会先确保 sessiontype / quality 枚举存在。

BEGIN;

DO $$
BEGIN
    CREATE TYPE sessiontype AS ENUM ('AM', 'FULL');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE TYPE quality AS ENUM ('official', 'provisional', 'estimated', 'fallback');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS market_index (
    id            SERIAL PRIMARY KEY,
    code          VARCHAR(16) NOT NULL,
    name_zh       VARCHAR(64) NOT NULL,
    name_en       VARCHAR(64),
    market        VARCHAR(16) NOT NULL,
    exchange      VARCHAR(32) NOT NULL,
    currency      VARCHAR(8)  NOT NULL DEFAULT 'HKD',
    timezone      VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    display_order INTEGER     NOT NULL DEFAULT 100,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_market_index_code UNIQUE (code)
);

INSERT INTO market_index (code, name_zh, name_en, market, exchange, currency, timezone, display_order)
VALUES
    ('HSI', '恒生指数', 'Hang Seng Index', 'HK', 'HKEX', 'HKD', 'Asia/Hong_Kong', 10),
    ('SSE', '上证指数', 'SSE Composite Index', 'CN', 'SSE', 'CNY', 'Asia/Shanghai', 20),
    ('SZSE', '深证成指', 'SZSE Component Index', 'CN', 'SZSE', 'CNY', 'Asia/Shanghai', 30)
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS index_quote_source_record (
    id                SERIAL PRIMARY KEY,
    index_id          INTEGER      NOT NULL REFERENCES market_index(id) ON DELETE CASCADE,
    trade_date        DATE         NOT NULL,
    session           sessiontype  NOT NULL,
    source            VARCHAR(32)  NOT NULL,
    last              INTEGER,
    change_points     INTEGER,
    change_pct        INTEGER,
    turnover_amount   BIGINT,
    turnover_currency VARCHAR(8),
    asof_ts           TIMESTAMPTZ,
    payload           JSONB,
    fetched_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    ok                BOOLEAN      NOT NULL DEFAULT TRUE,
    error             TEXT
);

CREATE INDEX IF NOT EXISTS ix_index_source_record_lookup
    ON index_quote_source_record (index_id, trade_date, session);

CREATE INDEX IF NOT EXISTS ix_index_source_record_source_fetched
    ON index_quote_source_record (source, fetched_at DESC);

CREATE TABLE IF NOT EXISTS index_quote_history (
    id                SERIAL PRIMARY KEY,
    index_id          INTEGER      NOT NULL REFERENCES market_index(id) ON DELETE CASCADE,
    trade_date        DATE         NOT NULL,
    session           sessiontype  NOT NULL,
    last              INTEGER      NOT NULL,
    change_points     INTEGER,
    change_pct        INTEGER,
    turnover_amount   BIGINT,
    turnover_currency VARCHAR(8)   NOT NULL DEFAULT 'HKD',
    best_source       VARCHAR(32)  NOT NULL,
    quality           quality      NOT NULL,
    source_count      INTEGER      NOT NULL DEFAULT 1,
    asof_ts           TIMESTAMPTZ,
    payload           JSONB,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_index_quote_history UNIQUE (index_id, trade_date, session)
);

CREATE INDEX IF NOT EXISTS ix_index_quote_history_index_date
    ON index_quote_history (index_id, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_index_quote_history_trade_session
    ON index_quote_history (trade_date, session);

CREATE TABLE IF NOT EXISTS index_realtime_snapshot (
    id                SERIAL PRIMARY KEY,
    index_id          INTEGER      NOT NULL REFERENCES market_index(id) ON DELETE CASCADE,
    trade_date        DATE         NOT NULL,
    session           sessiontype  NOT NULL,
    last              INTEGER      NOT NULL,
    change_points     INTEGER,
    change_pct        INTEGER,
    turnover_amount   BIGINT,
    turnover_currency VARCHAR(8)   NOT NULL DEFAULT 'HKD',
    data_updated_at   TIMESTAMPTZ  NOT NULL,
    is_closed         BOOLEAN      NOT NULL DEFAULT FALSE,
    source            VARCHAR(32)  NOT NULL,
    payload           JSONB,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_index_realtime_snapshot UNIQUE (index_id, trade_date)
);

CREATE INDEX IF NOT EXISTS ix_index_realtime_snapshot_date
    ON index_realtime_snapshot (trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_index_realtime_snapshot_index
    ON index_realtime_snapshot (index_id);

COMMIT;
