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
  curl -i http://localhost:8000/market-turnover/healthz

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
