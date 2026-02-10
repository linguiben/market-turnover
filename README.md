# market-turnover (mv)

Multi-source HK market turnover (trading value) collector + HSI price + web UI.

## What it does
- Fetches HK market turnover from multiple sources (planned: Tencent / AASTOCKS / HKEX)
- Normalizes into a Postgres DB (Docker)
- Computes last-30-trading-day distribution + today rank/percentile
- Provides a FastAPI web UI for dashboard, data inspection, and manual job triggers

## Quickstart
```bash
cp .env.example .env
docker compose up -d --build
# then open http://localhost:8000
```

## Status
MVP scaffold is in progress.
