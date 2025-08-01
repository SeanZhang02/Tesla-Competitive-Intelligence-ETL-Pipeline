"""Dashboard data service for optimized database queries."""
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import pandas as pd
from sqlalchemy import func, and_, desc
from sqlalchemy.orm import Session, joinedload

from config import (
    get_session_factory, Company, QuarterlyFinancial, AnalystEstimate,
    COMPANY_NAMES, settings
)

logger = logging.getLogger(__name__)


class DashboardDataService:
    """Service class for dashboard data operations with optimized queries."""
    
    def __init__(self):
        """Initialize the dashboard data service."""
        self.session_factory = get_session_factory()
    
    def get_companies_data(self) -> Dict[str, Dict]:
        """Get basic company information for dashboard display.
        
        Returns:
            Dict mapping ticker to company info
        """
        with self.session_factory() as session:
            companies = session.query(Company).filter(
                Company.ticker.in_(list(COMPANY_NAMES.keys()))
            ).all()
            
            return {
                company.ticker: {
                    'id': company.id,
                    'name': company.name,
                    'sector': company.sector,
                    'ticker': company.ticker
                }
                for company in companies
            }
    
    def get_quarterly_financials(
        self, 
        tickers: List[str] = None,
        start_date: date = None,
        end_date: date = None,
        metrics: List[str] = None
    ) -> pd.DataFrame:
        """Get quarterly financial data with flexible filtering.
        
        Args:
            tickers: List of tickers to include (default: all)
            start_date: Start date for filtering (default: 2 years ago)
            end_date: End date for filtering (default: today)
            metrics: Specific metrics to include (default: all)
            
        Returns:
            DataFrame with financial data
        """
        if tickers is None:
            tickers = list(COMPANY_NAMES.keys())
        
        if start_date is None:
            start_date = date.today() - timedelta(days=730)  # 2 years
        
        if end_date is None:
            end_date = date.today()
        
        with self.session_factory() as session:
            query = session.query(
                Company.ticker,
                Company.name,
                QuarterlyFinancial.quarter_date,
                QuarterlyFinancial.quarter_label,
                QuarterlyFinancial.revenue,
                QuarterlyFinancial.eps,
                QuarterlyFinancial.gross_profit,
                QuarterlyFinancial.updated_at
            ).join(
                Company, QuarterlyFinancial.company_id == Company.id
            ).filter(
                and_(
                    Company.ticker.in_(tickers),
                    QuarterlyFinancial.quarter_date >= start_date,
                    QuarterlyFinancial.quarter_date <= end_date
                )
            ).order_by(
                Company.ticker, QuarterlyFinancial.quarter_date
            )
            
            results = query.all()
            
            # Convert to DataFrame
            df = pd.DataFrame([
                {
                    'ticker': r.ticker,
                    'company_name': r.name,
                    'quarter_date': r.quarter_date,
                    'quarter_label': r.quarter_label,
                    'revenue': float(r.revenue) if r.revenue else None,
                    'eps': float(r.eps) if r.eps else None,
                    'gross_profit': float(r.gross_profit) if r.gross_profit else None,
                    'updated_at': r.updated_at
                }
                for r in results
            ])
            
            return df
    
    def get_performance_metrics(self, ticker: str = 'TSLA') -> Dict:
        """Get key performance metrics for a specific company.
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            Dict with performance metrics
        """
        with self.session_factory() as session:
            # Get latest quarter data
            latest_data = session.query(QuarterlyFinancial).join(
                Company
            ).filter(
                Company.ticker == ticker
            ).order_by(
                desc(QuarterlyFinancial.quarter_date)
            ).first()
            
            if not latest_data:
                return {}
            
            # Get previous quarter for comparison
            prev_data = session.query(QuarterlyFinancial).join(
                Company
            ).filter(
                and_(
                    Company.ticker == ticker,
                    QuarterlyFinancial.quarter_date < latest_data.quarter_date
                )
            ).order_by(
                desc(QuarterlyFinancial.quarter_date)
            ).first()
            
            # Calculate metrics
            metrics = {
                'latest_quarter': latest_data.quarter_label,
                'latest_revenue': float(latest_data.revenue) if latest_data.revenue else None,
                'latest_eps': float(latest_data.eps) if latest_data.eps else None,
                'latest_gross_profit': float(latest_data.gross_profit) if latest_data.gross_profit else None,
                'revenue_growth': None,
                'eps_growth': None,
                'gross_profit_growth': None
            }
            
            # Calculate growth rates if previous data exists
            if prev_data:
                if latest_data.revenue and prev_data.revenue:
                    metrics['revenue_growth'] = float(
                        (latest_data.revenue - prev_data.revenue) / prev_data.revenue * 100
                    )
                
                if latest_data.eps and prev_data.eps:
                    metrics['eps_growth'] = float(
                        (latest_data.eps - prev_data.eps) / prev_data.eps * 100
                    )
                
                if latest_data.gross_profit and prev_data.gross_profit:
                    metrics['gross_profit_growth'] = float(
                        (latest_data.gross_profit - prev_data.gross_profit) / prev_data.gross_profit * 100
                    )
            
            return metrics
    
    def get_comparison_data(
        self, 
        metric: str = 'revenue',
        quarters: int = 8
    ) -> pd.DataFrame:
        """Get comparison data across all companies for a specific metric.
        
        Args:
            metric: Financial metric to compare (revenue, eps, gross_profit)
            quarters: Number of recent quarters to include
            
        Returns:
            DataFrame formatted for chart visualization
        """
        with self.session_factory() as session:
            # Get recent quarters data
            subquery = session.query(
                QuarterlyFinancial.quarter_date
            ).distinct().order_by(
                desc(QuarterlyFinancial.quarter_date)
            ).limit(quarters).subquery()
            
            query = session.query(
                Company.ticker,
                Company.name,
                QuarterlyFinancial.quarter_label,
                QuarterlyFinancial.quarter_date,
                getattr(QuarterlyFinancial, metric)
            ).join(
                Company, QuarterlyFinancial.company_id == Company.id
            ).join(
                subquery, QuarterlyFinancial.quarter_date == subquery.c.quarter_date
            ).filter(
                Company.ticker.in_(list(COMPANY_NAMES.keys()))
            ).order_by(
                QuarterlyFinancial.quarter_date, Company.ticker
            )
            
            results = query.all()
            
            # Convert to DataFrame
            df = pd.DataFrame([
                {
                    'ticker': r.ticker,
                    'company_name': r.name,
                    'quarter_label': r.quarter_label,
                    'quarter_date': r.quarter_date,
                    'value': float(getattr(r, metric)) if getattr(r, metric) else None
                }
                for r in results
            ])
            
            return df
    
    def get_data_freshness(self) -> Dict[str, datetime]:
        """Get data freshness information for monitoring.
        
        Returns:
            Dict mapping ticker to last update timestamp
        """
        with self.session_factory() as session:
            query = session.query(
                Company.ticker,
                func.max(QuarterlyFinancial.updated_at).label('last_updated')
            ).join(
                Company, QuarterlyFinancial.company_id == Company.id
            ).filter(
                Company.ticker.in_(list(COMPANY_NAMES.keys()))
            ).group_by(
                Company.ticker
            )
            
            results = query.all()
            
            return {
                r.ticker: r.last_updated
                for r in results
            }
    
    def health_check(self) -> Dict[str, str]:
        """Perform health check on dashboard data service.
        
        Returns:
            Dict with health status information
        """
        try:
            with self.session_factory() as session:
                # Test database connection
                session.execute(func.now())
                
                # Check data availability
                company_count = session.query(Company).filter(
                    Company.ticker.in_(list(COMPANY_NAMES.keys()))
                ).count()
                
                financial_count = session.query(QuarterlyFinancial).join(
                    Company
                ).filter(
                    Company.ticker.in_(list(COMPANY_NAMES.keys()))
                ).count()
                
                return {
                    'status': 'healthy',
                    'database': 'connected',
                    'companies': f'{company_count} companies available',
                    'financial_records': f'{financial_count} records available'
                }
                
        except Exception as e:
            logger.error(f"Dashboard health check failed: {e}")
            return {
                'status': 'unhealthy',
                'database': 'connection_failed',
                'error': str(e)
            }