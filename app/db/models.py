from __future__ import annotations

import enum
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
    func,
)


def _enum_values(enum_cls):
    return [e.value for e in enum_cls]
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class SessionType(str, enum.Enum):
    AM = "AM"
    FULL = "FULL"


class Quality(str, enum.Enum):
    OFFICIAL = "official"
    PROVISIONAL = "provisional"
    ESTIMATED = "estimated"
    FALLBACK = "fallback"


class KlineInterval(str, enum.Enum):
    M1 = "1m"
    M5 = "5m"


class TradingCalendarHK(Base):
    __tablename__ = "trading_calendar_hk"

    trade_date = Column(Date, primary_key=True)
    is_trading_day = Column(Boolean, nullable=False, default=False)
    is_half_day = Column(Boolean, nullable=False, default=False)
    notes = Column(Text, nullable=True)


class TurnoverSourceRecord(Base):
    __tablename__ = "turnover_source_record"

    id = Column(Integer, primary_key=True)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)
    source = Column(String(32), nullable=False)  # HKEX/TENCENT/AASTOCKS

    turnover_hkd = Column(BigInteger, nullable=True)
    cutoff_time = Column(Time, nullable=True)

    asof_ts = Column(DateTime(timezone=True), nullable=True)
    payload = Column(JSONB, nullable=True)

    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    ok = Column(Boolean, nullable=False, default=True)
    error = Column(Text, nullable=True)


Index("ix_source_record_trade_session", TurnoverSourceRecord.trade_date, TurnoverSourceRecord.session)
Index("ix_source_record_source_fetched", TurnoverSourceRecord.source, TurnoverSourceRecord.fetched_at.desc())


class TurnoverFact(Base):
    __tablename__ = "turnover_fact"
    __table_args__ = (UniqueConstraint("trade_date", "session", name="uq_fact_trade_session"),)

    id = Column(Integer, primary_key=True)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)

    turnover_hkd = Column(BigInteger, nullable=False)
    cutoff_time = Column(Time, nullable=True)
    is_half_day_market = Column(Boolean, nullable=False, default=False)

    best_source = Column(String(32), nullable=False)
    quality = Column(Enum(Quality, name="quality", values_callable=_enum_values), nullable=False, default=Quality.PROVISIONAL)

    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index("ix_fact_session_trade_desc", TurnoverFact.session, TurnoverFact.trade_date.desc())


class HsiQuoteFact(Base):
    __tablename__ = "hsi_quote_fact"
    __table_args__ = (UniqueConstraint("trade_date", "session", name="uq_hsi_trade_session"),)

    id = Column(Integer, primary_key=True)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)

    last = Column(Integer, nullable=False)  # store *100 to keep 2 decimals w/o float
    change = Column(Integer, nullable=True)  # *100
    change_pct = Column(Integer, nullable=True)  # *100 (percent)

    turnover_hkd = Column(BigInteger, nullable=True)  # as reported by source feed
    asof_ts = Column(DateTime(timezone=True), nullable=True)

    source = Column(String(32), nullable=False, default="AASTOCKS")
    payload = Column(JSONB, nullable=True)

    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index("ix_hsi_trade_desc", HsiQuoteFact.session, HsiQuoteFact.trade_date.desc())


class MarketIndex(Base):
    __tablename__ = "market_index"
    __table_args__ = (UniqueConstraint("code", name="uq_market_index_code"),)

    id = Column(Integer, primary_key=True)
    code = Column(String(16), nullable=False)  # HSI/SSE/SZSE
    name_zh = Column(String(64), nullable=False)
    name_en = Column(String(64), nullable=True)
    market = Column(String(16), nullable=False)  # HK/CN
    exchange = Column(String(32), nullable=False)
    currency = Column(String(8), nullable=False, default="HKD")
    timezone = Column(String(64), nullable=False, default="Asia/Shanghai")
    is_active = Column(Boolean, nullable=False, default=True)
    display_order = Column(Integer, nullable=False, default=100)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class IndexQuoteSourceRecord(Base):
    __tablename__ = "index_quote_source_record"

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)
    source = Column(String(32), nullable=False)

    last = Column(Integer, nullable=True)  # *100
    change_points = Column(Integer, nullable=True)  # *100
    change_pct = Column(Integer, nullable=True)  # *100
    turnover_amount = Column(BigInteger, nullable=True)
    turnover_currency = Column(String(8), nullable=True)

    asof_ts = Column(DateTime(timezone=True), nullable=True)
    payload = Column(JSONB, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    ok = Column(Boolean, nullable=False, default=True)
    error = Column(Text, nullable=True)


Index(
    "ix_index_source_record_lookup",
    IndexQuoteSourceRecord.index_id,
    IndexQuoteSourceRecord.trade_date,
    IndexQuoteSourceRecord.session,
)
Index(
    "ix_index_source_record_source_fetched",
    IndexQuoteSourceRecord.source,
    IndexQuoteSourceRecord.fetched_at.desc(),
)


class IndexKlineSourceRecord(Base):
    __tablename__ = "index_kline_source_record"
    __table_args__ = (
        UniqueConstraint("index_id", "interval", "bar_time", "source", name="uq_index_kline_source"),
    )

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)
    interval = Column(Enum(KlineInterval, name="klineinterval", values_callable=_enum_values), nullable=False)
    bar_time = Column(DateTime(timezone=True), nullable=False)
    trade_date = Column(Date, nullable=False)
    source = Column(String(32), nullable=False)

    open = Column(Integer, nullable=True)  # *100
    high = Column(Integer, nullable=True)  # *100
    low = Column(Integer, nullable=True)  # *100
    close = Column(Integer, nullable=True)  # *100
    volume = Column(BigInteger, nullable=True)
    turnover_amount = Column(BigInteger, nullable=True)
    turnover_currency = Column(String(8), nullable=True)

    asof_ts = Column(DateTime(timezone=True), nullable=True)
    payload = Column(JSONB, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    ok = Column(Boolean, nullable=False, default=True)
    error = Column(Text, nullable=True)


Index(
    "ix_index_kline_lookup",
    IndexKlineSourceRecord.index_id,
    IndexKlineSourceRecord.interval,
    IndexKlineSourceRecord.bar_time.desc(),
)
Index(
    "ix_index_kline_trade_date",
    IndexKlineSourceRecord.trade_date,
    IndexKlineSourceRecord.interval,
)
Index(
    "ix_index_kline_source_fetched",
    IndexKlineSourceRecord.source,
    IndexKlineSourceRecord.fetched_at.desc(),
)


class IndexQuoteHistory(Base):
    __tablename__ = "index_quote_history"
    __table_args__ = (UniqueConstraint("index_id", "trade_date", "session", name="uq_index_quote_history"),)

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)

    last = Column(Integer, nullable=False)  # *100
    change_points = Column(Integer, nullable=True)  # *100
    change_pct = Column(Integer, nullable=True)  # *100

    turnover_amount = Column(BigInteger, nullable=True)
    turnover_currency = Column(String(8), nullable=False)

    best_source = Column(String(32), nullable=False)
    quality = Column(Enum(Quality, name="quality", values_callable=_enum_values), nullable=False, default=Quality.PROVISIONAL)
    source_count = Column(Integer, nullable=False, default=1)

    asof_ts = Column(DateTime(timezone=True), nullable=True)
    payload = Column(JSONB, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index("ix_index_quote_history_index_date", IndexQuoteHistory.index_id, IndexQuoteHistory.trade_date.desc())
Index("ix_index_quote_history_trade_session", IndexQuoteHistory.trade_date, IndexQuoteHistory.session)


class IndexIntradayBar(Base):
    __tablename__ = "index_intraday_bar"
    __table_args__ = (
        UniqueConstraint("index_id", "interval_min", "bar_ts", "source", name="uq_index_intraday_bar"),
    )

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)

    # Kline interval in minutes (1/5/15/...)
    interval_min = Column(Integer, nullable=False)

    # Bar timestamp (timezone-aware; stored as timestamptz)
    bar_ts = Column(DateTime(timezone=True), nullable=False)

    open = Column(Integer, nullable=True)  # *100
    high = Column(Integer, nullable=True)  # *100
    low = Column(Integer, nullable=True)  # *100
    close = Column(Integer, nullable=False)  # *100

    volume = Column(BigInteger, nullable=True)
    amount = Column(BigInteger, nullable=True)
    currency = Column(String(8), nullable=False)

    source = Column(String(32), nullable=False)
    payload = Column(JSONB, nullable=True)

    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


Index(
    "ix_index_intraday_bar_lookup",
    IndexIntradayBar.index_id,
    IndexIntradayBar.interval_min,
    IndexIntradayBar.bar_ts.desc(),
)


class IndexRealtimeSnapshot(Base):
    __tablename__ = "index_realtime_snapshot"
    # Append-only table: keep every snapshot row, never overwrite.

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)
    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)

    last = Column(Integer, nullable=False)  # *100
    change_points = Column(Integer, nullable=True)  # *100
    change_pct = Column(Integer, nullable=True)  # *100

    turnover_amount = Column(BigInteger, nullable=True)
    turnover_currency = Column(String(8), nullable=False)

    data_updated_at = Column(DateTime(timezone=True), nullable=False)
    is_closed = Column(Boolean, nullable=False, default=False)
    source = Column(String(32), nullable=False)
    payload = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index("ix_index_realtime_snapshot_date", IndexRealtimeSnapshot.trade_date.desc())
Index("ix_index_realtime_snapshot_index", IndexRealtimeSnapshot.index_id)
Index(
    "ix_index_realtime_snapshot_latest",
    IndexRealtimeSnapshot.index_id,
    IndexRealtimeSnapshot.trade_date,
    IndexRealtimeSnapshot.id.desc(),
)


class IndexRealtimeApiSnapshot(Base):
    __tablename__ = "index_realtime_api_snapshot"

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("market_index.id", ondelete="CASCADE"), nullable=False)
    code = Column(String(16), nullable=False)
    secid = Column(String(32), nullable=False)

    trade_date = Column(Date, nullable=False)
    session = Column(Enum(SessionType, name="sessiontype", values_callable=_enum_values), nullable=False)

    last = Column(Integer, nullable=True)  # *100
    change_points = Column(Integer, nullable=True)  # *100
    change_pct = Column(Integer, nullable=True)  # *100

    turnover_amount = Column(BigInteger, nullable=True)
    turnover_currency = Column(String(8), nullable=False, default="HKD")
    volume = Column(BigInteger, nullable=True)

    data_updated_at = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(32), nullable=False, default="EASTMONEY_STOCK_GET")
    payload = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


Index("ix_index_realtime_api_snapshot_date", IndexRealtimeApiSnapshot.trade_date.desc())
Index("ix_index_realtime_api_snapshot_index", IndexRealtimeApiSnapshot.index_id)
Index(
    "ix_index_realtime_api_snapshot_latest",
    IndexRealtimeApiSnapshot.index_id,
    IndexRealtimeApiSnapshot.trade_date,
    IndexRealtimeApiSnapshot.id.desc(),
)


class JobRun(Base):
    __tablename__ = "job_run"

    id = Column(Integer, primary_key=True)
    job_name = Column(String(64), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(16), nullable=False, default="running")  # running/success/failed/partial
    summary = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)


class AppCache(Base):
    __tablename__ = "app_cache"

    # Simple key-value cache persisted in DB.
    # Used for homepage widgets that refresh periodically.

    key = Column(String(128), primary_key=True)
    payload = Column(JSONB, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class InsightSysPrompt(Base):
    __tablename__ = "insight_sys_prompt"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    lang = Column(String(8), nullable=False)
    prompt_key = Column(String(64), nullable=False, default="market_insight")
    version = Column(String(32), nullable=False, default="v1")
    system_prompt = Column(Text, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index(
    "ix_insight_sys_prompt_lang_key_active",
    InsightSysPrompt.lang,
    InsightSysPrompt.prompt_key,
    InsightSysPrompt.is_active,
    InsightSysPrompt.updated_at.desc(),
)
Index(
    "uq_insight_sys_prompt_active_one",
    InsightSysPrompt.lang,
    InsightSysPrompt.prompt_key,
    unique=True,
    postgresql_where=InsightSysPrompt.is_active.is_(True),
)


class InsightSnapshot(Base):
    __tablename__ = "insight_snapshot"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False)
    asof_ts = Column(DateTime(timezone=True), nullable=False)
    lang = Column(String(8), nullable=False)
    peak_policy = Column(String(32), nullable=False, default="all_time")
    provider = Column(String(16), nullable=False)
    model = Column(String(64), nullable=False)
    prompt_version = Column(String(32), nullable=False, default="v1")
    payload = Column(JSONB, nullable=False)
    prompt = Column(Text, nullable=False)
    response = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="success")
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


Index("ix_insight_snapshot_asof_desc", InsightSnapshot.asof_ts.desc())
Index("ix_insight_snapshot_trade_date", InsightSnapshot.trade_date.desc())
Index("ix_insight_snapshot_lang_created_desc", InsightSnapshot.lang, InsightSnapshot.created_at.desc())


class AppUser(Base):
    __tablename__ = "app_user"
    __table_args__ = (
        CheckConstraint("username = email", name="ck_app_user_username_eq_email"),
        CheckConstraint("email = lower(email)", name="ck_app_user_email_lowercase"),
        CheckConstraint("username = lower(username)", name="ck_app_user_username_lowercase"),
        CheckConstraint(
            r"email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'",
            name="ck_app_user_email_format",
        ),
        UniqueConstraint("username", name="uq_app_user_username"),
        UniqueConstraint("email", name="uq_app_user_email"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    username = Column(String(320), nullable=False)
    email = Column(String(320), nullable=False)
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(64), nullable=True)
    is_active = Column(Boolean, nullable=False, default=False)
    is_superuser = Column(Boolean, nullable=False, default=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


Index("ix_app_user_active", AppUser.is_active)
Index("ix_app_user_created_at", AppUser.created_at.desc())


class UserVisitLog(Base):
    __tablename__ = "user_visit_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # nullable when unauthenticated
    user_id = Column(Integer, nullable=True)

    ip_address = Column(INET, nullable=False)
    session_id = Column(String(100), nullable=True)
    action_type = Column(String(20), nullable=True)  # login/visit/logout/...

    user_agent = Column(Text, nullable=True)
    browser_family = Column(String(50), nullable=True)
    os_family = Column(String(50), nullable=True)
    device_type = Column(String(20), nullable=True)  # pc/mobile/tablet

    request_url = Column(Text, nullable=False)
    referer_url = Column(Text, nullable=True)

    request_headers = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


Index("idx_visit_logs_created_at", UserVisitLog.created_at.desc())
Index("idx_visit_logs_user_id", UserVisitLog.user_id, postgresql_where=UserVisitLog.user_id.isnot(None))
Index("idx_visit_logs_referer", UserVisitLog.referer_url, postgresql_where=UserVisitLog.referer_url.isnot(None))
Index(
    "idx_visit_logs_ip",
    UserVisitLog.ip_address,
    postgresql_using="gist",
    postgresql_ops={"ip_address": "inet_ops"},
)
