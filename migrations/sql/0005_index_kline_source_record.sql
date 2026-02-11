-- 0005_index_kline_source_record.sql
-- 对应 Alembic 版本: 0004_index_kline_source_record

BEGIN;

DO $$
BEGIN
    CREATE TYPE klineinterval AS ENUM ('1m', '5m');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS index_kline_source_record (
    id                SERIAL PRIMARY KEY,
    index_id          INTEGER       NOT NULL REFERENCES market_index(id) ON DELETE CASCADE,
    interval          klineinterval NOT NULL,
    bar_time          TIMESTAMPTZ   NOT NULL,
    trade_date        DATE          NOT NULL,
    source            VARCHAR(32)   NOT NULL,
    open              INTEGER,
    high              INTEGER,
    low               INTEGER,
    close             INTEGER,
    volume            BIGINT,
    turnover_amount   BIGINT,
    turnover_currency VARCHAR(8),
    asof_ts           TIMESTAMPTZ,
    payload           JSONB,
    fetched_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    ok                BOOLEAN       NOT NULL DEFAULT TRUE,
    error             TEXT,
    CONSTRAINT uq_index_kline_source UNIQUE (index_id, interval, bar_time, source)
);

CREATE INDEX IF NOT EXISTS ix_index_kline_lookup
    ON index_kline_source_record (index_id, interval, bar_time DESC);

CREATE INDEX IF NOT EXISTS ix_index_kline_trade_date
    ON index_kline_source_record (trade_date, interval);

CREATE INDEX IF NOT EXISTS ix_index_kline_source_fetched
    ON index_kline_source_record (source, fetched_at DESC);

COMMIT;

