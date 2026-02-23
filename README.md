# market-turnover (mv)

Multi-source HK market turnover (trading value) collector + HSI price + web UI.

## What it does
- Fetches HK market turnover from multiple sources (planned: Tencent / AASTOCKS / HKEX)
- Normalizes into a Postgres DB (Docker)
- Computes last-30-trading-day distribution + today rank/percentile
- Provides a FastAPI web UI for dashboard, data inspection, and manual job triggers

## Quickstart
```bash
cp env.example .env
docker compose up -d --build
# then open http://localhost:8000
```

## 外部数据源（URL整理）

以下为项目当前代码中实际使用到的外部市场数据源接口/页面：

| 数据源 | 主要用途 | URL |
| --- | --- | --- |
| Tushare Pro | 指数日线（HSI/SSE/SZSE）与部分分钟K线（SDK） | https://api.tushare.pro |
| Eastmoney Suggest API | 代码/QuoteID 解析（如 HSI -> secid） | https://searchapi.eastmoney.com/api/suggest/get |
| Eastmoney Kline API | 指数分钟线、盘中快照与成交额聚合 | https://push2his.eastmoney.com/api/qt/stock/kline/get |
| Tencent 行情接口 | 指数日线/实时行情补充 | https://web.ifzq.gtimg.cn/appstock/app/fqkline/get |
| Tencent 即时行情接口 | 多标的实时报价 | https://qt.gtimg.cn/q= |
| AASTOCKS 指数数据页 | HSI 全日成交抓取（页面解析） | https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx |
| AASTOCKS 指数数据接口 | HSI 快照（价格/涨跌/成交） | https://www.aastocks.com/tc/resources/datafeed/getstockindex.ashx?type=5 |
| HKEX 统计归档 JSON | 港股历史全日成交回填 | https://www.hkex.com.hk/eng/stat/smstat/mthbull/rpt_data_statistics_archive_trading_data_{start}_{end}.json |

对应代码位置（便于后续维护）：
- `app/sources/tushare_index.py`, `app/sources/tushare_kline.py`
- `app/sources/eastmoney_index.py`, `app/sources/eastmoney_intraday.py`
- `app/sources/tencent_index.py`, `app/services/tencent_quote.py`
- `app/sources/aastocks.py`, `app/sources/aastocks_index.py`
- `app/sources/hkex.py`

## 数据源分析
```sql
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

------ cn index ----

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

## Scheduled jobs switch
- `ENABLE_SCHEDULED_JOBS=false` (default): do not start the APScheduler cron scheduler.
- `ENABLE_SCHEDULED_JOBS=true`: start cron scheduler on app startup.
  - `fetch_intraday_snapshot`: Mon-Fri, 09:00-17:00, every 5 minutes (includes 17:00)
  - `fetch_am`: Mon-Fri, 11:35
  - `fetch_full`: Mon-Fri, 16:10
  - `fetch_tushare_index`: daily 20:00

## 作业与定时任务总览

| 作业名 (`job_name`) | 简介 | 运行频率 | 控制方式 | 备注/参数 |
| --- | --- | --- | --- | --- |
| `fetch_intraday_snapshot` | 抓取 HSI/SSE/SZSE 盘中快照，写入 `index_realtime_snapshot` | 工作日 09:00-17:00，每5分钟（含 17:00） | 定时 + 手动（Jobs 页 / `POST /api/jobs/run`） | 可传 `codes`、`force_source` |
| `fetch_am` | 午盘成交与快照，同步最新 Tushare 日线 | 工作日 11:35（cron） | 定时 + 手动（Jobs 页 / API） | 无必填参数 |
| `fetch_full` | 全日成交与快照，同步最新 Tushare 日线 | 工作日 16:10（cron） | 定时 + 手动（Jobs 页 / API） | 无必填参数 |
| `fetch_tushare_index` | 同步 HSI/SSE/SZSE 最新日线 | 每日 20:00（cron） | 定时 + 手动（Jobs 页 / API） | 无必填参数 |
| `fetch_intraday_bars_cn_5m` | 保存 A 股 5 分钟K线到 `index_intraday_bar` | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 可传 `lookback_days` |
| `backfill_tushare_index` | 回填指数历史日线（当前代码为近 365 天） | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 无必填参数 |
| `backfill_cn_halfday` | 用 Eastmoney 回填 A 股半日/全日成交 | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 无必填参数 |
| `backfill_intraday_kline` | 回填分钟K线源数据（1m/5m） | 无自动调度（按需执行） | 手动（API） | 当前未在 Jobs 页面列出 |
| `persist_eastmoney_kline_all` | 持久化 Eastmoney 可获取范围内的 1m/5m K线 | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 可传 `lookback_days_1m`、`lookback_days_5m` |
| `backfill_hsi_am_from_kline` | 从 5m K线聚合回填 HSI 半日成交 | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 可传 `date_from`、`date_to` |
| `backfill_hkex` | 从 HKEX 官方统计回填历史全日成交 | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 无必填参数 |
| `backfill_hsi_am_yesterday` | 回填 HSI 指定日（默认昨日）半日成交 | 无自动调度（按需执行） | 手动（Jobs 页 / API） | 可传 `trade_date` |

说明:
- Jobs 页面: `http://localhost:8000/market-turnover/jobs`
- API 触发: `POST /market-turnover/api/jobs/run`（或根路径挂载时 `POST /api/jobs/run`）

## Tushare Pro datasource
- Configure `TUSHARE_PRO_TOKEN` in `.env` (from https://tushare.pro).
- Optional: `TUSHARE_PRO_BASE`, `TUSHARE_TIMEOUT_SECONDS`, `TUSHARE_INDEX_CODES`.
- Run job `fetch_tushare_index` for latest daily quotes.
- Run job `backfill_tushare_index` to backfill the latest 90 days.
- `fetch_full` / `fetch_am` also try syncing latest Tushare index data.

## Status
MVP scaffold is in progress.

## Maintain
```bash
python3 -m venv .venv # Create venv
source .venv/bin/activate # Activate venv
pip install -r requirements.txt # Install dependencies
deactivate # Deactivate venv
```

###### loca测试方式 ######
数据库迁移: alembic upgrade head。
1. Docker 方式

  docker compose up -d --build  
  docker compose ps  
  docker compose logs --tail=100 web  

  2. 本地 venv 方式

  source .venv/bin/activate  
  uvicorn app.main:app --host 0.0.0.0 --port 8000

  启动后先测：

  curl -i http://localhost:8000/healthz
  curl -i http://localhost:8000/market-turnover

  再打开：
  http://localhost:8000/market-turnover/（建议带末尾 /）。

###### 执行数据抓取任务 ######
• 可在 Jobs 页面查看可用任务并点击 `Run` 触发：
  `http://localhost:8000/market-turnover/jobs`
• 也可以通过 POST 接口触发。
  1. 先确保服务已启动

  docker compose up -d --build
  2. 触发任务（任选）

  # 午盘抓取
  curl -X POST -F 'job_name=fetch_am' http://localhost:8000/market-turnover/api/jobs/run

  # 全日抓取
  curl -X POST -F 'job_name=fetch_full' http://localhost:8000/market-turnover/api/jobs/run
  # 同步指数（日线，Tushare）
  curl -X POST -F 'job_name=fetch_tushare_index' http://localhost:8000/market-turnover/api/jobs/run

  # 回填最近90天指数（日线，Tushare）
  curl -X POST -F 'job_name=backfill_tushare_index' http://localhost:8000/market-turnover/api/jobs/run

  # 回填分钟K线（SSE/SZSE：1m近2天 + 5m近90天）
  curl -X POST -F 'job_name=backfill_intraday_kline' http://localhost:8000/market-turnover/api/jobs/run

  # 回填HKEX历史
  curl -X POST -F 'job_name=backfill_hkex' http://localhost:8000/market-turnover/api/jobs/run
  # from https://www.hkex.com.hk/Market-Data/Statistics/Consolidated-Reports/
    sc_lang=zh-HK

  3. 查看执行结果
     打开：http://localhost:8000/market-turnover/jobs

  建议先跑：backfill_hkex -> fetch_tushare_index -> fetch_full。
