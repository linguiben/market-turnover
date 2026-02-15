```sql
  SQL
-- 1：查询 HSI 半日峰值（与主页逻辑一致，含回退）
  WITH hsi AS (
    SELECT id
    FROM market_index
    WHERE code = 'HSI'
    LIMIT 1
  ),
  hist_am AS (
    SELECT MAX(h.turnover_amount)::bigint AS peak_am_hist
    FROM index_quote_history h -- 历史表
    JOIN hsi ON h.index_id = hsi.id
    WHERE h.session = 'AM'::sessiontype
      AND h.turnover_amount IS NOT NULL
  ),
  fact_am AS (
    SELECT MAX(tf.turnover_hkd)::bigint AS peak_am_fact
    FROM turnover_fact tf  -- hkex官网数据
    WHERE tf.session = 'AM'::sessiontype
      AND tf.turnover_hkd IS NOT NULL
  )
  SELECT
    COALESCE(hist_am.peak_am_hist, fact_am.peak_am_fact) AS peak_am_raw,
    ROUND(COALESCE(hist_am.peak_am_hist, fact_am.peak_am_fact) / 1000000000.0, 2) AS peak_am_b,
    CASE
      WHEN hist_am.peak_am_hist IS NOT NULL THEN 'index_quote_history(HSI,AM)'
      ELSE 'turnover_fact(AM)'
    END AS source_used
  FROM hist_am
  CROSS JOIN fact_am;

  SQL 2：查询 HSI 全日峰值（仪表盘 PEAK）
  SELECT
    MAX(tf.turnover_hkd)::bigint AS peak_full_raw,
    ROUND(MAX(tf.turnover_hkd) / 1000000000.0, 2) AS peak_full_b,
    'turnover_fact(FULL)' AS source_used
  FROM turnover_fact tf
  WHERE tf.session = 'FULL'::sessiontype
    AND tf.turnover_hkd IS NOT NULL;
```
```

# HSI
## 1.“全日成交”
market_index 
-> index_realtime_snapshot # todo: 检查哪个job写入
-> index_quote_history # todo: 检查哪个job写入
-> turnover_fact (HSI专属) # todo: 检查哪个job写入

## 2.“当日成交” todo: 1和2的逻辑应该保持一致 
-> index_realtime_snapshot (session=AM 且 data_updated_at<=12:30)
-> index_quote_history

## 3.“历史均值
-> index_quote_history.turnover_amount session='AM'/'PM'
-> turnover_fact.turnover_hkd session='AM'/'FULL'

#  4.“价格
-> index_realtime_snapshot
-> index_quote_history
-> hsi_quote_fact (HSI专属)

---

# SSE, SZSE
## 1) “全日成交”
-> index_realtime_snapshot session=FULL
-> # todo: 不拿历史?

## 2) 当日成交（图里的“当日成交”两根柱）
-> index_realtime_snapshot session=AM # todo: 只拿AM?
-> index_quote_history session=AM

## 3) “历史均值”
-> index_quote_history session='AM'/'FULL'

## 4) “价格”
-> index_realtime_snapshot.last -> index_quote_history.last
```