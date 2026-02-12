from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

# NOTE:
# Default SQLAlchemy QueuePool is small (pool_size=5). This app has:
# - request DB sessions (FastAPI dependency)
# - background APScheduler jobs
# - analytics visit log writes (background thread)
# Under moderate traffic this can exhaust the pool and make the site appear down.
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=getattr(settings, "DB_POOL_SIZE", 20),
    max_overflow=getattr(settings, "DB_MAX_OVERFLOW", 40),
    pool_timeout=getattr(settings, "DB_POOL_TIMEOUT", 30),
    pool_recycle=getattr(settings, "DB_POOL_RECYCLE", 1800),
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
