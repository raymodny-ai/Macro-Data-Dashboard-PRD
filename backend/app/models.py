"""
ORM 模型 - TimescaleDB 超表映射
架构原则: 五张超表 + UPSERT 联合唯一约束
"""
from datetime import date, datetime
from sqlalchemy import (
    Column, Date, String, Numeric, SmallInteger, Boolean, DateTime,
    UniqueConstraint, Index
)
from app.database import Base


class InflationData(Base):
    """通胀二阶导组"""
    __tablename__ = "inflation_data"
    __table_args__ = (
        UniqueConstraint("record_date", "symbol", name="uq_inflation"),
        Index("idx_inflation_date", "record_date"),
    )

    record_date = Column(Date, primary_key=True, nullable=False)
    symbol = Column(String(32), primary_key=True, nullable=False)
    value = Column(Numeric(16, 6))
    mom_growth = Column(Numeric(12, 8))
    acceleration = Column(Numeric(12, 8))
    three_mma = Column(Numeric(12, 8))
    warning_flag = Column(Boolean, default=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class FiscalAuctionData(Base):
    """财政增量组"""
    __tablename__ = "fiscal_auction_data"
    __table_args__ = (
        UniqueConstraint("auction_date", "security_type", name="uq_fiscal"),
        Index("idx_fiscal_date", "auction_date"),
    )

    auction_date = Column(Date, primary_key=True, nullable=False)
    security_type = Column(String(16), primary_key=True, nullable=False)
    bid_to_cover_ratio = Column(Numeric(8, 4))
    high_yield = Column(Numeric(8, 6))
    expected_yield = Column(Numeric(8, 6))
    tail_spread = Column(Numeric(8, 6))
    acm_term_premium = Column(Numeric(8, 6))
    fiscal_warning_flag = Column(Boolean, default=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class LiquidityCorridor(Base):
    """流动性走廊组"""
    __tablename__ = "liquidity_corridor"
    __table_args__ = (
        UniqueConstraint("record_date", "symbol", name="uq_liquidity"),
        Index("idx_liquidity_date", "record_date"),
        Index("idx_liquidity_state", "system_state", "record_date"),
    )

    record_date = Column(Date, primary_key=True, nullable=False)
    symbol = Column(String(16), primary_key=True, nullable=False, default="SPREAD")
    sofr_rate = Column(Numeric(8, 6))
    iorb_rate = Column(Numeric(8, 6))
    spread = Column(Numeric(8, 6))
    system_state = Column(SmallInteger)
    crisis_alert = Column(Boolean, default=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class AiCapexData(Base):
    """AI 资本开支组"""
    __tablename__ = "ai_capex_data"
    __table_args__ = (
        UniqueConstraint("report_date", "company_cik", name="uq_ai_capex"),
        Index("idx_ai_capex_date", "report_date"),
    )

    report_date = Column(Date, primary_key=True, nullable=False)
    company_cik = Column(String(16), primary_key=True, nullable=False)
    company_name = Column(String(64))
    capex = Column(Numeric(18, 2))
    rd_expense = Column(Numeric(18, 2))
    capex_mom = Column(Numeric(12, 6))
    capex_yoy = Column(Numeric(12, 6))
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class MarketContagion(Base):
    """市场传染组"""
    __tablename__ = "market_contagion"
    __table_args__ = (
        UniqueConstraint("trade_date", "symbol", name="uq_contagion"),
        Index("idx_contagion_date", "trade_date"),
        Index("idx_contagion_alert", "contagion_alert", "trade_date"),
    )

    trade_date = Column(Date, primary_key=True, nullable=False)
    symbol = Column(String(16), primary_key=True, nullable=False)
    close_price = Column(Numeric(12, 4))
    log_return = Column(Numeric(12, 8))
    move_index = Column(Numeric(10, 4))
    rolling_corr_30d = Column(Numeric(10, 8))
    rolling_corr_60d = Column(Numeric(10, 8))
    contagion_alert = Column(Boolean, default=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)
