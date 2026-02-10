-- 0004_index_quote_views.sql
-- 用途: 为指数看板提供可直接查询的视图

BEGIN;

-- 1) 指数最新快照（卡片展示）
CREATE OR REPLACE VIEW vw_index_latest_snapshot AS
SELECT
    mi.id AS index_id,
    mi.code,
    mi.name_zh,
    mi.name_en,
    mi.market,
    mi.exchange,
    mi.display_order,
    irs.trade_date,
    irs.session,
    irs.last,
    (irs.last::numeric / 100) AS last_price,
    irs.change_points,
    (irs.change_points::numeric / 100) AS change_points_value,
    irs.change_pct,
    (irs.change_pct::numeric / 100) AS change_pct_value,
    irs.turnover_amount,
    COALESCE(irs.turnover_currency, mi.currency) AS turnover_currency,
    irs.data_updated_at,
    irs.is_closed,
    irs.source
FROM index_realtime_snapshot irs
JOIN market_index mi ON mi.id = irs.index_id
WHERE mi.is_active = TRUE;

-- 2) 图表指标（半日/全日当日成交、5日均值、10日均值、历史峰值、点位峰值）
CREATE OR REPLACE VIEW vw_index_turnover_chart_metrics AS
WITH latest_trade AS (
    SELECT
        mi.id AS index_id,
        COALESCE(
            (SELECT MAX(rs.trade_date) FROM index_realtime_snapshot rs WHERE rs.index_id = mi.id),
            (SELECT MAX(h.trade_date) FROM index_quote_history h WHERE h.index_id = mi.id)
        ) AS trade_date
    FROM market_index mi
    WHERE mi.is_active = TRUE
),
hist_ranked AS (
    SELECT
        h.index_id,
        h.session,
        h.trade_date,
        h.turnover_amount,
        ROW_NUMBER() OVER (PARTITION BY h.index_id, h.session ORDER BY h.trade_date DESC) AS rn
    FROM index_quote_history h
),
point_peak AS (
    SELECT
        h.index_id,
        MAX(h.last) AS max_points
    FROM index_quote_history h
    GROUP BY h.index_id
),
turnover_peak AS (
    SELECT
        h.index_id,
        MAX(CASE WHEN h.session = 'AM'::sessiontype THEN h.turnover_amount END) AS max_vol_am,
        MAX(CASE WHEN h.session = 'FULL'::sessiontype THEN h.turnover_amount END) AS max_vol_day
    FROM index_quote_history h
    GROUP BY h.index_id
)
SELECT
    mi.id AS index_id,
    mi.code,
    mi.name_zh,
    mi.display_order,
    lt.trade_date,
    COALESCE(rs.last, h_full.last, h_am.last) AS today_points,
    pp.max_points,
    COALESCE(h_am.turnover_amount, CASE WHEN rs.session = 'AM'::sessiontype THEN rs.turnover_amount END) AS today_vol_am,
    COALESCE(h_full.turnover_amount, CASE WHEN rs.session = 'FULL'::sessiontype THEN rs.turnover_amount END) AS today_vol_day,
    (
        SELECT AVG(hr.turnover_amount)::bigint
        FROM hist_ranked hr
        WHERE hr.index_id = mi.id
          AND hr.session = 'AM'::sessiontype
          AND hr.rn <= 5
    ) AS avg_vol_am_5d,
    (
        SELECT AVG(hr.turnover_amount)::bigint
        FROM hist_ranked hr
        WHERE hr.index_id = mi.id
          AND hr.session = 'FULL'::sessiontype
          AND hr.rn <= 5
    ) AS avg_vol_day_5d,
    (
        SELECT AVG(hr.turnover_amount)::bigint
        FROM hist_ranked hr
        WHERE hr.index_id = mi.id
          AND hr.session = 'AM'::sessiontype
          AND hr.rn <= 10
    ) AS avg_vol_am_10d,
    (
        SELECT AVG(hr.turnover_amount)::bigint
        FROM hist_ranked hr
        WHERE hr.index_id = mi.id
          AND hr.session = 'FULL'::sessiontype
          AND hr.rn <= 10
    ) AS avg_vol_day_10d,
    tp.max_vol_am,
    tp.max_vol_day,
    COALESCE(rs.data_updated_at, h_full.asof_ts, h_am.asof_ts) AS data_updated_at
FROM market_index mi
JOIN latest_trade lt ON lt.index_id = mi.id
LEFT JOIN index_realtime_snapshot rs
       ON rs.index_id = mi.id
      AND rs.trade_date = lt.trade_date
LEFT JOIN index_quote_history h_am
       ON h_am.index_id = mi.id
      AND h_am.trade_date = lt.trade_date
      AND h_am.session = 'AM'::sessiontype
LEFT JOIN index_quote_history h_full
       ON h_full.index_id = mi.id
      AND h_full.trade_date = lt.trade_date
      AND h_full.session = 'FULL'::sessiontype
LEFT JOIN point_peak pp ON pp.index_id = mi.id
LEFT JOIN turnover_peak tp ON tp.index_id = mi.id
WHERE lt.trade_date IS NOT NULL
  AND mi.is_active = TRUE;

-- 3) 全局数据同步状态（页面右上角 Last Data Sync）
CREATE OR REPLACE VIEW vw_index_data_sync_status AS
SELECT
    MAX(v.data_updated_at) AS last_data_sync_at,
    MAX(v.trade_date) AS last_trade_date,
    COUNT(*) AS active_index_count
FROM vw_index_latest_snapshot v;

COMMIT;
