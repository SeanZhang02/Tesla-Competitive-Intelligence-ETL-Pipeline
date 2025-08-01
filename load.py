"""
Data loading module for Tesla Competitive Intelligence ETL Pipeline.
Handles bulk loading of financial data into PostgreSQL with proper transaction management.
"""
import logging
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
from decimal import Decimal

import pandas as pd
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from config import (
    setup_logging, get_session_factory, Company, QuarterlyFinancial, 
    AnalystEstimate, FinancialData, EstimateData
)

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Raised when data loading operations fail."""
    pass


class DatabaseLoader:
    """Handles loading financial data into PostgreSQL with transaction management."""
    
    def __init__(self):
        self.session_factory = get_session_factory()
        self.company_cache = {}
        
    @contextmanager
    def get_session(self):
        """Context manager for database sessions with automatic rollback on error."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error, rolling back: {e}")
            raise
        finally:
            session.close()
    
    def load_companies(self, tickers: List[str] = None) -> Dict[str, int]:
        """Load companies into database with upsert logic."""
        if tickers is None:
            tickers = ['TSLA', 'RIVN', 'LCID']
        
        company_names = {
            'TSLA': 'Tesla Inc',
            'RIVN': 'Rivian Automotive Inc', 
            'LCID': 'Lucid Group Inc'
        }
        
        company_mapping = {}
        
        with self.get_session() as session:
            try:
                existing_companies = session.execute(select(Company).where(Company.ticker.in_(tickers))).fetchall()
                existing_tickers = {company.Company.ticker: company.Company.id for company in existing_companies}
                
                new_companies = [{
                    'ticker': ticker, 'name': company_names.get(ticker, f'{ticker} Inc'), 'sector': 'Electric Vehicles'
                } for ticker in tickers if ticker not in existing_tickers]
                
                if new_companies:
                    session.execute(insert(Company), new_companies)
                    logger.info(f"Inserted {len(new_companies)} new companies")
                
                all_companies = session.execute(select(Company).where(Company.ticker.in_(tickers))).fetchall()
                company_mapping = {company.Company.ticker: company.Company.id for company in all_companies}
                self.company_cache.update(company_mapping)
                
                logger.info(f"Loaded companies: {list(company_mapping.keys())}")
                return company_mapping
                
            except Exception as e:
                logger.error(f"Failed to load companies: {e}")
                raise LoadError(f"Company loading failed: {e}")
    
    def load_quarterly_financials(self, financial_data: List[FinancialData]) -> int:
        """Bulk load quarterly financial data using SQLAlchemy 2.0 syntax."""
        if not financial_data:
            logger.warning("No financial data to load")
            return 0
        
        # Ensure companies are loaded first
        unique_tickers = list(set([data.ticker for data in financial_data]))
        company_mapping = self.company_cache or self.load_companies(unique_tickers)
        
        with self.get_session() as session:
            try:
                records = []
                skipped = 0
                
                for data in financial_data:
                    company_id = company_mapping.get(data.ticker)
                    if not company_id:
                        logger.warning(f"Company ID not found for {data.ticker}, skipping")
                        skipped += 1
                        continue
                    
                    records.append({
                        'company_id': company_id,
                        'quarter_date': data.quarter_date,
                        'quarter_label': data.quarter_label,
                        'revenue': float(data.revenue) if data.revenue else None,
                        'eps': float(data.eps) if data.eps else None,
                        'gross_profit': float(data.gross_profit) if data.gross_profit else None
                    })
                
                if not records:
                    logger.warning("No valid records to load after processing")
                    return 0
                
                # Use SQLAlchemy 2.0 bulk insert with ON CONFLICT handling
                try:
                    # First, try to insert all records
                    result = session.execute(insert(QuarterlyFinancial), records)
                    loaded_count = len(records)
                    
                except IntegrityError as e:
                    logger.info("Handling duplicate records with upsert logic")
                    session.rollback()
                    
                    loaded_count = 0
                    for record in records:
                        try:
                            existing = session.execute(
                                select(QuarterlyFinancial).where(
                                    QuarterlyFinancial.company_id == record['company_id'],
                                    QuarterlyFinancial.quarter_date == record['quarter_date']
                                )
                            ).first()
                            
                            if existing:
                                session.execute(
                                    QuarterlyFinancial.__table__.update().where(
                                        QuarterlyFinancial.id == existing.QuarterlyFinancial.id
                                    ).values(**record)
                                )
                            else:
                                session.execute(insert(QuarterlyFinancial), [record])
                            loaded_count += 1
                        except Exception as record_error:
                            logger.warning(f"Failed to process record {record['quarter_label']}: {record_error}")
                            continue
                    session.commit()
                
                logger.info(f"Successfully loaded {loaded_count} financial records ({skipped} skipped)")
                return loaded_count
                
            except Exception as e:
                logger.error(f"Failed to load quarterly financials: {e}")
                raise LoadError(f"Financial data loading failed: {e}")
    
    def load_analyst_estimates(self, estimate_data: List[EstimateData]) -> int:
        """Load analyst estimates data into database."""
        if not estimate_data:
            logger.warning("No estimate data to load")
            return 0
        
        # Ensure companies are loaded first
        unique_tickers = list(set([data.ticker for data in estimate_data]))
        company_mapping = self.company_cache or self.load_companies(unique_tickers)
        
        with self.get_session() as session:
            try:
                records = []
                for data in estimate_data:
                    company_id = company_mapping.get(data.ticker)
                    if not company_id:
                        continue
                    
                    record = {
                        'company_id': company_id,
                        'quarter_date': data.quarter_date,
                        'quarter_label': data.quarter_label,
                        'estimated_revenue': float(data.estimated_revenue) if data.estimated_revenue else None,
                        'estimated_eps': float(data.estimated_eps) if data.estimated_eps else None,
                        'analyst_count': data.analyst_count
                    }
                    records.append(record)
                
                if records:
                    session.execute(insert(AnalystEstimate), records)
                    logger.info(f"Loaded {len(records)} analyst estimate records")
                    return len(records)
                
                return 0
                
            except Exception as e:
                logger.error(f"Failed to load analyst estimates: {e}")
                raise LoadError(f"Estimate data loading failed: {e}")
    
    def load_from_dataframe(self, df: pd.DataFrame) -> int:
        """Load financial data from pandas DataFrame."""
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return 0
        
        try:
            financial_data = []
            for _, row in df.iterrows():
                try:
                    revenue = Decimal(str(row['revenue'])) if pd.notnull(row['revenue']) else None
                    eps = Decimal(str(row['eps'])) if pd.notnull(row['eps']) else None
                    gross_profit = Decimal(str(row['gross_profit'])) if pd.notnull(row['gross_profit']) else None
                    
                    financial_data.append(FinancialData(
                        ticker=row['ticker'], quarter_date=pd.to_datetime(row['quarter_date']).date(),
                        quarter_label=row['quarter_label'], revenue=revenue, eps=eps, gross_profit=gross_profit
                    ))
                except Exception as e:
                    logger.warning(f"Failed to convert DataFrame row to FinancialData: {e}")
                    continue
            return self.load_quarterly_financials(financial_data)
            
        except Exception as e:
            logger.error(f"Failed to load from DataFrame: {e}")
            raise LoadError(f"DataFrame loading failed: {e}")
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of loaded data for validation."""
        with self.get_session() as session:
            try:
                company_counts = {}
                companies = session.execute(select(Company)).fetchall()
                
                for company in companies:
                    ticker = company.Company.ticker
                    financial_count = session.execute(
                        select(QuarterlyFinancial).where(QuarterlyFinancial.company_id == company.Company.id)
                    ).fetchall()
                    company_counts[ticker] = {'financial_records': len(financial_count), 'company_id': company.Company.id}
                
                return {'total_companies': len(companies), 'company_breakdown': company_counts, 'last_updated': pd.Timestamp.now().isoformat()}
            except Exception as e:
                logger.error(f"Failed to get data summary: {e}")
                return {'error': str(e)}
    
    def validate_tesla_data(self) -> bool:
        """Validate Tesla Q2 2025 data in database matches expected values."""
        with self.get_session() as session:
            try:
                tesla = session.execute(select(Company).where(Company.ticker == 'TSLA')).first()
                if not tesla:
                    logger.error("Tesla company not found in database")
                    return False
                
                tesla_q2_2025 = session.execute(
                    select(QuarterlyFinancial).where(
                        QuarterlyFinancial.company_id == tesla.Company.id,
                        QuarterlyFinancial.quarter_label == '2025-Q2'
                    )
                ).first()
                
                if not tesla_q2_2025:
                    logger.warning("Tesla Q2 2025 data not found in database")
                    return False
                
                record = tesla_q2_2025.QuarterlyFinancial
                expected_revenue = 22500000000.0
                tolerance_revenue = expected_revenue * 0.001
                
                if record.revenue:
                    revenue_diff = abs(float(record.revenue) - expected_revenue)
                    if revenue_diff > tolerance_revenue:
                        logger.error(f"Tesla Q2 2025 revenue validation failed: Expected ${expected_revenue:,.0f}, got ${record.revenue:,.0f}")
                        return False
                
                expected_eps = 0.3709  # Updated based on actual Q2 2025 data
                if record.eps and abs(float(record.eps) - expected_eps) > 0.01:
                    logger.error(f"Tesla Q2 2025 EPS validation failed: Expected ${expected_eps}, got ${record.eps}")
                    return False
                
                logger.info("Tesla Q2 2025 database validation passed")
                return True
            except Exception as e:
                logger.error(f"Database validation failed: {e}")
                return False


if __name__ == "__main__":
    loader = DatabaseLoader()
    company_mapping = loader.load_companies(['TSLA', 'RIVN', 'LCID'])
    print(f"Company mapping: {company_mapping}")
    summary = loader.get_data_summary()
    print(f"Data summary: {summary}")