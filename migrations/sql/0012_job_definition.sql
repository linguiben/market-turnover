CREATE TABLE IF NOT EXISTS job_definition (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(64) NOT NULL UNIQUE,
    handler_name VARCHAR(64) NOT NULL,
    label_zh VARCHAR(128) NOT NULL,
    label_en VARCHAR(128) NULL,
    description_zh TEXT NOT NULL,
    description_en TEXT NULL,
    targets JSONB NOT NULL DEFAULT '[]'::jsonb,
    params_schema JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    manual_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    schedule_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ui_order INTEGER NOT NULL DEFAULT 100,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_job_name_format CHECK (job_name ~ '^[a-z][a-z0-9_]{1,63}$'),
    CONSTRAINT ck_handler_name_format CHECK (handler_name ~ '^[a-z][a-z0-9_]{1,63}$'),
    CONSTRAINT ck_targets_array CHECK (jsonb_typeof(targets) = 'array'),
    CONSTRAINT ck_params_schema_array CHECK (jsonb_typeof(params_schema) = 'array'),
    CONSTRAINT ck_default_params_object CHECK (jsonb_typeof(default_params) = 'object')
);

CREATE INDEX IF NOT EXISTS ix_job_definition_active_order
    ON job_definition (is_active, ui_order, job_name);

CREATE TABLE IF NOT EXISTS job_schedule (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(64) NOT NULL REFERENCES job_definition(job_name) ON DELETE CASCADE,
    schedule_code VARCHAR(64) NOT NULL,
    trigger_type VARCHAR(16) NOT NULL DEFAULT 'cron',
    timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
    second VARCHAR(32) NOT NULL DEFAULT '0',
    minute VARCHAR(32) NOT NULL DEFAULT '*',
    hour VARCHAR(32) NOT NULL DEFAULT '*',
    day VARCHAR(32) NOT NULL DEFAULT '*',
    month VARCHAR(32) NOT NULL DEFAULT '*',
    day_of_week VARCHAR(32) NOT NULL DEFAULT '*',
    start_date TIMESTAMPTZ NULL,
    end_date TIMESTAMPTZ NULL,
    jitter_seconds INTEGER NULL,
    misfire_grace_time INTEGER NOT NULL DEFAULT 120,
    coalesce BOOLEAN NOT NULL DEFAULT TRUE,
    max_instances INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    description VARCHAR(255) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_job_schedule UNIQUE (job_name, schedule_code),
    CONSTRAINT ck_job_schedule_trigger_type CHECK (trigger_type IN ('cron')),
    CONSTRAINT ck_job_schedule_jitter CHECK (jitter_seconds IS NULL OR jitter_seconds BETWEEN 0 AND 3600),
    CONSTRAINT ck_job_schedule_misfire CHECK (misfire_grace_time BETWEEN 1 AND 86400),
    CONSTRAINT ck_job_schedule_max_instances CHECK (max_instances BETWEEN 1 AND 20),
    CONSTRAINT ck_job_schedule_date_range CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date)
);

CREATE INDEX IF NOT EXISTS ix_job_schedule_active_job
    ON job_schedule (is_active, job_name);

CREATE INDEX IF NOT EXISTS ix_job_schedule_job
    ON job_schedule (job_name);
