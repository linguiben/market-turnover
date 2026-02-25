CREATE TABLE IF NOT EXISTS insight_sys_prompt (
    id BIGSERIAL PRIMARY KEY,
    lang VARCHAR(8) NOT NULL CHECK (lang IN ('zh', 'en')),
    prompt_key VARCHAR(64) NOT NULL DEFAULT 'market_insight',
    version VARCHAR(32) NOT NULL DEFAULT 'v1',
    system_prompt TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_insight_sys_prompt_lang_key_active
    ON insight_sys_prompt (lang, prompt_key, is_active, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_insight_sys_prompt_active_one
    ON insight_sys_prompt (lang, prompt_key)
    WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS insight_snapshot (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    asof_ts TIMESTAMPTZ NOT NULL,
    lang VARCHAR(8) NOT NULL CHECK (lang IN ('zh', 'en')),
    peak_policy VARCHAR(32) NOT NULL DEFAULT 'all_time',
    provider VARCHAR(16) NOT NULL,
    model VARCHAR(64) NOT NULL,
    prompt_version VARCHAR(32) NOT NULL DEFAULT 'v1',
    payload JSONB NOT NULL,
    prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'success',
    error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_insight_snapshot_asof_desc
    ON insight_snapshot (asof_ts DESC);

CREATE INDEX IF NOT EXISTS ix_insight_snapshot_trade_date
    ON insight_snapshot (trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_insight_snapshot_lang_created_desc
    ON insight_snapshot (lang, created_at DESC);
