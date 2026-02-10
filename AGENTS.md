# AGENTS.md

## Repo Overview
- App: FastAPI dashboard + job runner for HK market turnover data.
- Services: Postgres (Docker) + web app (Docker).
- Base path: `/market-turnover` (set via `BASE_PATH`).

## Setup
1. Copy env file and adjust as needed:
```bash
cp env.example .env
```
2. Create the external Postgres volume used by docker-compose:
```bash
docker volume create hk-turnover_pgdata
```
3. Build and start services:
```bash
docker compose up -d --build
```

## Run / Access
- Web UI: `http://localhost:8000/market-turnover`
- Health check (root): `http://localhost:8000/healthz`
- Health check (base path): `http://localhost:8000/market-turnover/healthz`

## Key Environment Variables (.env)
- `BASE_PATH=/market-turnover`
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`
- `DATABASE_URL=postgresql+psycopg://...@db:5432/...`
- `TZ=Asia/Shanghai`
- Data-source config: `TENCENT_API_KEY`, `TENCENT_API_BASE`, `AASTOCKS_TIMEOUT_SECONDS`, `HKEX_TIMEOUT_SECONDS`

## Jobs
Jobs are triggered from the UI (Dashboard or Jobs page) or via the POST endpoint.
- Available job: `fetch_am` (midday session fetch)
- Available job: `fetch_full` (full-day fetch)

Manual trigger endpoint:
```bash
curl -X POST -F 'job_name=fetch_am' http://localhost:8000/market-turnover/api/jobs/run
curl -X POST -F 'job_name=fetch_full' http://localhost:8000/market-turnover/api/jobs/run
```

## Notes
- `docker-compose.yml` expects the `hk-turnover_pgdata` volume to exist.
