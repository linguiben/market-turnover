from __future__ import annotations

import enum
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
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


class JobRun(Base):
    __tablename__ = "job_run"

    id = Column(Integer, primary_key=True)
    job_name = Column(String(64), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(16), nullable=False, default="running")  # running/success/failed/partial
    summary = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)
