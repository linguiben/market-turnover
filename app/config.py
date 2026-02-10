from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "market-turnover"
    TZ: str = "Asia/Shanghai"

    # If you expose behind reverse proxy at /market-turnover
    BASE_PATH: str = ""  # e.g. "/market-turnover"

    BASIC_AUTH_USER: str | None = None
    BASIC_AUTH_PASS: str | None = None

    DATABASE_URL: str

    CUTOFF_TIME_AM: str = "12:00:00"  # HH:MM:SS
    RECENT_TRADING_DAYS: int = 30
    SOURCE_PRIORITY: str = "HKEX,TENCENT,AASTOCKS"

    # Tencent
    TENCENT_API_KEY: str | None = None
    TENCENT_API_BASE: str | None = None

    # AASTOCKS
    AASTOCKS_TIMEOUT_SECONDS: int = 10

    # HKEX
    HKEX_TIMEOUT_SECONDS: int = 20


settings = Settings()
