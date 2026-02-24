from __future__ import annotations

from urllib.parse import quote_plus

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "market-turnover"
    TZ: str = "Asia/Shanghai"
    ENABLE_SCHEDULED_JOBS: bool = False

    # Database pool tuning (avoid QueuePool exhaustion under traffic + scheduled jobs)
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 40
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800

    # If you expose behind reverse proxy at /market-turnover
    BASE_PATH: str = ""  # e.g. "/market-turnover"

    BASIC_AUTH_USER: str | None = None
    BASIC_AUTH_PASS: str | None = None
    AUTH_SECRET_KEY: str = "dev-only-change-me"
    AUTH_SESSION_MAX_AGE_SECONDS: int = 7 * 24 * 3600

    DATABASE_URL: str | None = None
    POSTGRES_DB: str | None = None
    POSTGRES_USER: str | None = None
    POSTGRES_PASSWORD: str | None = None
    POSTGRES_HOST: str = "127.0.0.1"
    POSTGRES_PORT: int = 5432

    CUTOFF_TIME_AM: str = "12:00:00"  # HH:MM:SS
    RECENT_TRADING_DAYS: int = 30
    # Source priority for daily/aggregated facts.
    # Jupiter preference: prefer Tushare Pro for all indices when available.
    SOURCE_PRIORITY: str = "TUSHARE,HKEX,EASTMONEY,TENCENT,AASTOCKS"

    # Scheduler
    # ENABLE_SCHEDULED_JOBS controls whether APScheduler cron jobs start in FastAPI lifespan.

    # Tencent
    TENCENT_API_KEY: str | None = None
    TENCENT_API_BASE: str | None = None

    # Tushare Pro
    TUSHARE_PRO_TOKEN: str | None = None
    TUSHARE_PRO_BASE: str = "https://api.tushare.pro"
    TUSHARE_TIMEOUT_SECONDS: int = 15
    # Format: CODE=TUSHARE_TS_CODE (global indices without dot use index_global API, cn indices with .SH/.SZ use index_daily)
    # CN indices: SSE=000001.SH, SZSE=399001.SZ
    # Global indices (index_global): HSI, DJI, IXIC, SPX, FTSE, GDAXI, N225, KS11, CSX5P
    TUSHARE_INDEX_CODES: str = "HSI=HSI.HI,SSE=000001.SH,SZSE=399001.SZ,DJI=DJI,IXIC=IXIC,SPX=SPX,FTSE=FTSE,GDAXI=GDAXI,N225=N225,KS11=KS11,CSX5P=CSX5P"

    # AASTOCKS
    AASTOCKS_TIMEOUT_SECONDS: int = 10

    # HKEX
    HKEX_TIMEOUT_SECONDS: int = 20

    @model_validator(mode="after")
    def resolve_database_url(self) -> Settings:
        raw_url = (self.DATABASE_URL or "").strip()
        if raw_url and "POSTGRES_" not in raw_url:
            self.DATABASE_URL = raw_url
            return self

        if self.POSTGRES_DB and self.POSTGRES_USER:
            auth = self.POSTGRES_USER
            if self.POSTGRES_PASSWORD:
                auth = f"{auth}:{quote_plus(self.POSTGRES_PASSWORD)}"
            self.DATABASE_URL = (
                f"postgresql+psycopg://{auth}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
            return self

        raise ValueError(
            "DATABASE_URL is not set. Provide DATABASE_URL or POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_HOST/POSTGRES_PORT."
        )

    def tushare_index_map(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for part in self.TUSHARE_INDEX_CODES.split(","):
            part = part.strip()
            if not part or "=" not in part:
                continue
            code, ts_code = part.split("=", 1)
            code = code.strip().upper()
            ts_code = ts_code.strip().upper()
            if code and ts_code:
                result[code] = ts_code
        return result


settings = Settings()
