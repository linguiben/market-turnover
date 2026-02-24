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

---

# Development Guide for AI Agents

## Build, Lint, and Test Commands

### Virtual Environment Setup
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Running the Application

#### Docker (Recommended)
```bash
# Build and start all services
docker compose up -d --build

# View logs
docker compose logs --tail=100 web

# Restart service
docker compose restart web
```

#### Local Development
```bash
# With venv activated
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Database Migrations
```bash
# Run migrations (inside container or with venv)
alembic upgrade head

# Create new migration
alembic revision --autogenerate -m "description"

# Rollback one version
alembic downgrade -1
```

### Linting and Type Checking
This project does not have a formal linting configuration. Use basic Python tools:

```bash
# Syntax check
python3 -m py_compile app/main.py

# Install optional linters (if needed)
pip install ruff basedpyright

# Run ruff linter
ruff check app/

# Run type checker (if basedpyright installed)
basedpyright app/
```

### Testing
No formal test framework is configured. For ad-hoc testing:

```bash
# Test a single module imports correctly
python3 -c "from app.main import app; print('OK')"

# Test database connection
python3 -c "from app.db.session import engine; print(engine)"
```

---

## Code Style Guidelines

### General Principles
- Write clean, readable code over clever code
- Follow existing patterns in the codebase
- Keep functions focused and small (under 50 lines when possible)
- Use descriptive variable and function names

### Imports

**Order (top to bottom):**
1. Standard library (`from datetime import datetime`)
2. Third-party packages (`from fastapi import APIRouter`)
3. Local application imports (`from app.config import settings`)

**Example:**
```python
from __future__ import annotations

import ipaddress
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from app.config import settings
from fastapi import APIRouter, Depends, Form, Request
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import User, IndexQuoteHistory
from app.services.my_service import my_function
from app.web.routes import router
```

### Type Hints

- Use type hints for all function parameters and return types
- Use `from __future__ import annotations` for forward references
- Prefer explicit types over `Any`
- Use `| None` syntax (Python 3.10+) over `Optional[]`

**Good:**
```python
def process_data(user_id: int, name: str | None = None) -> dict[str, Any]:
    pass
```

**Avoid:**
```python
def process_data(user_id, name=None):  # No types
    pass
```

### Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Functions/methods | snake_case | `get_user_by_id()` |
| Variables | snake_case | `user_name` |
| Classes | PascalCase | `IndexQuoteHistory` |
| Constants | UPPER_SNAKE_CASE | `MAX_RETRY_COUNT` |
| Database tables | snake_case | `index_quote_history` |
| HTTP routes | kebab-case | `/api/jobs/run` |

### Error Handling

- Use specific exception types rather than catching `Exception`
- Never use empty catch blocks: `except Exception: pass`
- Log errors with context before re-raising or returning
- Use `try/except` only when you can handle the error meaningfully

**Good:**
```python
try:
    result = fetch_data()
except ValueError as e:
    logger.warning(f"Invalid data format: {e}")
    return None
except httpx.HTTPError as e:
    logger.error(f"HTTP error fetching data: {e}")
    raise
```

**Avoid:**
```python
try:
    result = fetch_data()
except:  # Too broad
    pass
```

### Database Operations

- Use SQLAlchemy ORM with explicit queries
- Always commit or use session context managers
- Use parameterized queries (SQLAlchemy handles this automatically)
- Close sessions properly (use dependency injection via `get_db`)

**Good:**
```python
def get_user(db: Session, user_id: int) -> User | None:
    return db.query(User).filter(User.id == user_id).first()

# In FastAPI dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

### FastAPI Patterns

- Use dependency injection for database sessions
- Define response models for API endpoints
- Use appropriate HTTP methods (GET, POST, etc.)
- Group related endpoints in routers

**Example:**
```python
router = APIRouter()

@router.get("/items/{item_id}", response_model=ItemResponse)
def read_item(item_id: int, db: Session = Depends(get_db)):
    item = get_item(db, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
```

### Configuration

- Use `pydantic-settings` for configuration
- Never hardcode secrets or configuration values
- Use environment variables via `.env` file

**Example:**
```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    
    DATABASE_URL: str = "postgresql://localhost/db"
    SECRET_KEY: str
```

### File Organization

```
app/
├── __init__.py
├── config.py           # Configuration settings
├── main.py             # FastAPI app entry point
├── db/
│   ├── models.py      # SQLAlchemy models
│   └── session.py     # Database session management
├── services/           # Business logic
├── sources/           # External data source integrations
├── jobs/              # Background job definitions
├── web/
│   ├── routes.py      # API routes
│   ├── templates/     # Jinja2 templates
│   └── static/        # Static assets
└── migrations/        # Alembic migrations
```

### Jinja2 Templates

- Use Tailwind CSS classes for styling
- Follow existing template patterns
- Keep template logic minimal (do heavy processing in Python)
- Use `tojson` filter for passing data to JavaScript

### Git Conventions

- Make small, focused commits
- Write descriptive commit messages (imperative mood)
- Don't commit secrets or `.env` files

### Docker Development

- Use `docker compose up -d --build` to rebuild
- Check logs with `docker compose logs -f web`
- Access container shell: `docker compose exec web sh`
- Run commands in container: `docker compose exec web alembic upgrade head`

---

## Common Development Tasks

### Adding a New Job
1. Add job logic in `app/jobs/tasks.py`
2. Register in `AVAILABLE_JOBS` tuple in `app/web/routes.py`
3. Add cron schedule in `app/main.py` (if needed)

### Adding a New Database Model
1. Add model in `app/db/models.py`
2. Create migration: `alembic revision --autogenerate -m "add new table"`
3. Run migration: `alembic upgrade head`

### Adding a New API Endpoint
1. Add route in `app/web/routes.py` or create new router file
2. Use dependency injection for DB session
3. Return appropriate response model

### Adding a New External Data Source
1. Create source module in `app/sources/`
2. Follow existing patterns in other source files
3. Add to job tasks as needed
