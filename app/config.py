from __future__ import annotations

from urllib.parse import quote_plus

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "market-turnover"
    TZ: str = "Asia/Shanghai"
    ENABLE_SCHEDULED_JOBS: bool = False

    # If you expose behind reverse proxy at /market-turnover
    BASE_PATH: str = ""  # e.g. "/market-turnover"

    BASIC_AUTH_USER: str | None = None
    BASIC_AUTH_PASS: str | None = None

    DATABASE_URL: str | None = None
    POSTGRES_DB: str | None = None
    POSTGRES_USER: str | None = None
    POSTGRES_PASSWORD: str | None = None
    POSTGRES_HOST: str = "127.0.0.1"
    POSTGRES_PORT: int = 5432

    CUTOFF_TIME_AM: str = "12:00:00"  # HH:MM:SS
    RECENT_TRADING_DAYS: int = 30
    SOURCE_PRIORITY: str = "HKEX,TENCENT,AASTOCKS,TUSHARE"

    # Tencent
    TENCENT_API_KEY: str | None = None
    TENCENT_API_BASE: str | None = None

    # Tushare Pro
    TUSHARE_PRO_TOKEN: str | None = None
    TUSHARE_PRO_BASE: str = "https://api.tushare.pro"
    TUSHARE_TIMEOUT_SECONDS: int = 15
    TUSHARE_INDEX_CODES: str = "HSI=HSI.HI,SSE=000001.SH,SZSE=399001.SZ"

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
