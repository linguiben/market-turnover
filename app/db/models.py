from __future__ import annotations

import enum
from sqlalchemy import (
    BigInteger,
    Boolean,
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
from sqlalchemy.dialects.postgresql import JSONB
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
    __table_args__ = (UniqueConstraint("index_id", "trade_date", name="uq_index_realtime_snapshot"),)

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


class JobRun(Base):
    __tablename__ = "job_run"

    id = Column(Integer, primary_key=True)
    job_name = Column(String(64), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(16), nullable=False, default="running")  # running/success/failed/partial
    summary = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)
