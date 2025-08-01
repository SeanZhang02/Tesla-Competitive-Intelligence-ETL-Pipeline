"""Configuration module for Tesla ETL Pipeline."""
import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from sqlalchemy import (
    Column, Integer, String, Date, ForeignKey, Index, DECIMAL,
    create_engine, TIMESTAMP, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# Database base
Base = declarative_base()


class Settings(BaseSettings):
    """Application settings with environment variable validation."""
    fmp_api_key: str = Field(..., description="Financial Modeling Prep API key")
    database_url: str = Field(default="postgresql://localhost:5432/competitor_intelligence")
    api_rate_limit: int = Field(default=250)
    log_level: str = Field(default="INFO")
    
    # Dashboard-specific settings
    dashboard_port: int = Field(default=8501, description="Streamlit dashboard port")
    dashboard_cache_ttl: int = Field(default=300, description="Cache TTL in seconds (5 minutes)")
    dashboard_auto_refresh: int = Field(default=300, description="Auto refresh interval in seconds")
    dashboard_page_title: str = Field(default="Tesla Competitive Intelligence Dashboard")

    class Config:
        env_file = ".env"


# SQLAlchemy ORM Models
class Company(Base):
    """Company model for storing EV company information."""
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    ticker = Column(String(10), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    sector = Column(String(50), default='Electric Vehicles')
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))


class QuarterlyFinancial(Base):
    """Model for quarterly financial data."""
    __tablename__ = 'quarterly_financials'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    quarter_date = Column(Date, nullable=False)
    quarter_label = Column(String(10), nullable=False)
    revenue = Column(DECIMAL(15, 2), nullable=True)
    eps = Column(DECIMAL(10, 4), nullable=True)
    gross_profit = Column(DECIMAL(15, 2), nullable=True)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    __table_args__ = (Index('idx_company_quarter', 'company_id', 'quarter_date', unique=True),)


class AnalystEstimate(Base):
    """Model for analyst estimates data."""
    __tablename__ = 'analyst_estimates'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    quarter_date = Column(Date, nullable=False)
    quarter_label = Column(String(10), nullable=False)
    estimated_revenue = Column(DECIMAL(15, 2), nullable=True)
    estimated_eps = Column(DECIMAL(10, 4), nullable=True)
    analyst_count = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    __table_args__ = (Index('idx_estimates_company_quarter', 'company_id', 'quarter_date', unique=True),)


# Pydantic Models for Validation
class FinancialData(BaseModel):
    """Validated financial data model with proper decimal handling."""
    
    ticker: str = Field(..., min_length=1, max_length=10)
    quarter_date: date
    quarter_label: str = Field(..., pattern=r'^\d{4}-Q[1-4]$')
    revenue: Optional[Decimal] = Field(None, max_digits=15, decimal_places=2)
    eps: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4)
    gross_profit: Optional[Decimal] = Field(None, max_digits=15, decimal_places=2)
    
    @field_validator('revenue', 'gross_profit', mode='before')
    @classmethod
    def convert_millions_to_dollars(cls, v):
        """Convert from millions to actual dollar amounts if needed."""
        if v is not None and isinstance(v, (int, float, Decimal)):
            # If value is less than 1M, assume it's in millions
            if Decimal(str(v)) < Decimal('1000000'):
                return Decimal(str(v)) * Decimal('1000000')
        return v


class EstimateData(BaseModel):
    """Validated analyst estimate data model."""
    
    ticker: str = Field(..., min_length=1, max_length=10)
    quarter_date: date
    quarter_label: str = Field(..., pattern=r'^\d{4}-Q[1-4]$')
    estimated_revenue: Optional[Decimal] = Field(None, max_digits=15, decimal_places=2)
    estimated_eps: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4)
    analyst_count: Optional[int] = Field(None, ge=0)


# Database Configuration
def get_database_engine():
    """Create SQLAlchemy engine with connection pooling."""
    settings = Settings()
    
    engine = create_engine(
        settings.database_url,
        pool_size=20,
        pool_recycle=3600,
        pool_use_lifo=True,
        pool_pre_ping=True,
        echo=False
    )
    return engine


def get_session_factory():
    """Create SQLAlchemy session factory."""
    engine = get_database_engine()
    return sessionmaker(bind=engine)


def setup_logging():
    """Configure logging with file and console handlers."""
    logger = logging.getLogger()
    log_level = 'INFO' if not settings else settings.log_level
    logger.setLevel(getattr(logging, log_level))
    
    # File and console handlers
    fh = RotatingFileHandler('logs/etl_pipeline.log', maxBytes=10485760, backupCount=5)
    ch = logging.StreamHandler()
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    ch.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    
    logger.addHandler(fh)
    logger.addHandler(ch)


# Dashboard Constants
COMPANY_NAMES = {
    'TSLA': 'Tesla Inc',
    'RIVN': 'Rivian Automotive Inc',
    'LCID': 'Lucid Group Inc'
}

COMPANY_COLORS = {
    'TSLA': '#E31E24',  # Tesla red
    'RIVN': '#0F4C75',  # Rivian blue
    'LCID': '#2E8B57'   # Lucid green
}

DASHBOARD_METRICS = {
    'revenue': {'label': 'Revenue', 'format': '$', 'unit': 'B'},
    'eps': {'label': 'Earnings Per Share', 'format': '$', 'unit': ''},
    'gross_profit': {'label': 'Gross Profit', 'format': '$', 'unit': 'B'}
}

# Global settings instance - handle missing env vars for testing
try: settings = Settings()
except Exception: settings = None