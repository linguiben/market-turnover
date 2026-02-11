-- 0007_user_visit_logs.sql
-- User visit & login audit logs
BEGIN;

CREATE TABLE IF NOT EXISTS user_visit_logs (
  id BIGSERIAL PRIMARY KEY, -- 自增主键

  user_id INT, -- 用户ID，未登录时可为空
  ip_address INET NOT NULL, -- IPv4/IPv6

  session_id VARCHAR(100), -- 会话ID，用于追踪同一批次的操作
  action_type VARCHAR(20), -- 操作类型：'login', 'visit', 'logout' 等

  -- 浏览器与设备信息
  user_agent TEXT, -- 原始用户代理字符串
  browser_family VARCHAR(50), -- 浏览器名称，如 Chrome, Firefox
  os_family VARCHAR(50), -- 操作系统，如 Windows, macOS, Android
  device_type VARCHAR(20), -- 设备类型：pc, mobile, tablet

  -- 来源与路径
  request_url TEXT NOT NULL, -- 当前请求的完整URL
  referer_url TEXT, -- 来源页面URL

  -- 扩展数据
  request_headers JSONB, -- 以JSON格式存储请求头（注意脱敏 Authorization/Cookie 等）

  -- 时间戳
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- 记录创建时间
);

-- 1. 按时间排序：几乎所有的日志查询都会用到时间范围
CREATE INDEX IF NOT EXISTS idx_visit_logs_created_at ON user_visit_logs (created_at DESC);

-- 2. 按用户追踪：查询特定用户的所有登录行为
CREATE INDEX IF NOT EXISTS idx_visit_logs_user_id ON user_visit_logs (user_id) WHERE user_id IS NOT NULL;

-- 3. 安全审计：快速定位特定IP地址的活动记录（支持网段/范围）
CREATE INDEX IF NOT EXISTS idx_visit_logs_ip ON user_visit_logs USING GIST (ip_address inet_ops);

-- 4. 来源分析：统计哪些外部链接带来的流量最多
CREATE INDEX IF NOT EXISTS idx_visit_logs_referer ON user_visit_logs (referer_url) WHERE referer_url IS NOT NULL;

-- 说明: 用户表 DDL（用户名使用 email 地址）

CREATE TABLE IF NOT EXISTS app_user (
    id              BIGSERIAL PRIMARY KEY,
    username        VARCHAR(320) NOT NULL, -- 用户名，必须为 email
    email           VARCHAR(320) NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    display_name    VARCHAR(64),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    is_superuser    BOOLEAN      NOT NULL DEFAULT FALSE,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- 强约束: username 与 email 必须一致，并统一小写存储
    CONSTRAINT ck_app_user_username_eq_email CHECK (username = email),
    CONSTRAINT ck_app_user_email_lowercase CHECK (email = lower(email)),
    CONSTRAINT ck_app_user_username_lowercase CHECK (username = lower(username)),
    CONSTRAINT ck_app_user_email_format CHECK (
        email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    ),

    CONSTRAINT uq_app_user_username UNIQUE (username),
    CONSTRAINT uq_app_user_email UNIQUE (email)
);

CREATE INDEX IF NOT EXISTS ix_app_user_active
    ON app_user (is_active);

CREATE INDEX IF NOT EXISTS ix_app_user_created_at
    ON app_user (created_at DESC);

-- 用户行为累计计数（全局一行）
CREATE TABLE IF NOT EXISTS user_activity_counter (
    id              SMALLINT    PRIMARY KEY DEFAULT 1,
    visit_count     BIGINT      NOT NULL DEFAULT 0, -- 访问次数
    login_count     BIGINT      NOT NULL DEFAULT 0, -- 登录次数
    last_visit_at   TIMESTAMPTZ,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_user_activity_counter_singleton CHECK (id = 1),
    CONSTRAINT ck_user_activity_counter_visit_nonneg CHECK (visit_count >= 0),
    CONSTRAINT ck_user_activity_counter_login_nonneg CHECK (login_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_user_activity_counter_updated_at
    ON user_activity_counter (updated_at DESC);

-- 用户行为按日计数（全局每天一行）
CREATE TABLE IF NOT EXISTS user_activity_counter_daily (
    stat_date       DATE        PRIMARY KEY,
    visit_count     BIGINT      NOT NULL DEFAULT 0,
    login_count     BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_user_activity_daily_visit_nonneg CHECK (visit_count >= 0),
    CONSTRAINT ck_user_activity_daily_login_nonneg CHECK (login_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_user_activity_counter_daily_date
    ON user_activity_counter_daily (stat_date DESC);

CREATE INDEX IF NOT EXISTS ix_user_activity_counter_daily_updated_at
    ON user_activity_counter_daily (updated_at DESC);

COMMIT;
