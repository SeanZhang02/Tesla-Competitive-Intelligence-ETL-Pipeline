"""
Data extraction module for Tesla Competitive Intelligence ETL Pipeline.
Handles API calls to Financial Modeling Prep and yfinance fallback.
"""
import json
import logging
import os
import time
from typing import Dict, List, Optional, Any

import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings, setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when API rate limit is exceeded."""
    pass


class APIError(Exception):
    """Raised when API calls fail."""
    pass


class FMPExtractor:
    """Financial Modeling Prep API extractor with rate limiting and retry logic."""
    
    def __init__(self, api_key: str = None, rate_limit: int = 250):
        self.api_key = api_key or settings.fmp_api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.rate_limit = rate_limit
        self.daily_calls = 0
        self.session = self._create_session()
        
        # Ensure raw data directory exists
        os.makedirs('data/raw', exist_ok=True)
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _check_rate_limit(self):
        """Check if we've exceeded the daily rate limit."""
        if self.daily_calls >= self.rate_limit:
            raise RateLimitError(f"Daily API limit of {self.rate_limit} calls reached")
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict:
        """Make API request with error handling."""
        self._check_rate_limit()
        
        # Add API key to params
        params['apikey'] = self.api_key
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            logger.info(f"Making API request to {endpoint} with params: {params}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            self.daily_calls += 1
            data = response.json()
            
            if not data:
                logger.warning(f"Empty response from {endpoint}")
                return {}
            
            logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else 1} records from {endpoint}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            raise APIError(f"Failed to fetch data from {endpoint}: {e}")
    
    def get_quarterly_income_statement(self, ticker: str, limit: int = 8) -> Dict:
        """Extract quarterly income statement data for a given ticker."""
        endpoint = f"income-statement/{ticker}"
        params = {"period": "quarter", "limit": limit}
        
        try:
            data = self._make_request(endpoint, params)
            save_path = f"data/raw/{ticker}_income_raw.json"
            with open(save_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved raw income data to {save_path}")
            return data
        except Exception as e:
            logger.error(f"Failed to get income statement for {ticker}: {e}")
            raise
    
    def get_analyst_estimates(self, ticker: str, limit: int = 4) -> Dict:
        """Extract analyst estimates data for a given ticker."""
        endpoint = f"analyst-estimates/{ticker}"
        params = {"period": "quarter", "limit": limit}
        
        try:
            data = self._make_request(endpoint, params)
            save_path = f"data/raw/{ticker}_estimates_raw.json"
            with open(save_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved raw estimates data to {save_path}")
            return data
        except Exception as e:
            logger.error(f"Failed to get analyst estimates for {ticker}: {e}")
            raise


class YFinanceExtractor:
    """yfinance fallback extractor for when FMP API fails or limits are hit."""
    
    def __init__(self):
        # Ensure raw data directory exists
        os.makedirs('data/raw', exist_ok=True)
    
    def get_quarterly_income_statement(self, ticker: str, limit: int = 8) -> Dict:
        """Extract quarterly income statement using yfinance."""
        try:
            logger.info(f"Fetching {ticker} data using yfinance fallback")
            stock = yf.Ticker(ticker)
            income_data = stock.quarterly_income_stmt
            
            if income_data.empty:
                logger.warning(f"No income data available for {ticker} via yfinance")
                return {}
            
            formatted_data = self._format_yfinance_data(income_data, ticker)
            save_path = f"data/raw/{ticker}_income_yf_raw.json"
            with open(save_path, 'w') as f:
                json.dump(formatted_data, f, indent=2)
            logger.info(f"Saved yfinance income data to {save_path}")
            return formatted_data
        except Exception as e:
            logger.error(f"yfinance extraction failed for {ticker}: {e}")
            raise APIError(f"yfinance fallback failed for {ticker}: {e}")
    
    def _format_yfinance_data(self, data, ticker: str) -> List[Dict]:
        """Convert yfinance data to FMP-compatible format."""
        formatted_records = []
        
        for date_col in data.columns[:8]:  # Limit to 8 quarters
            try:
                # Extract financial metrics
                revenue = data.loc['Total Revenue', date_col] if 'Total Revenue' in data.index else None
                gross_profit = data.loc['Gross Profit', date_col] if 'Gross Profit' in data.index else None
                
                # Calculate basic EPS (simplified)
                net_income = data.loc['Net Income', date_col] if 'Net Income' in data.index else None
                
                record = {
                    'date': date_col.strftime('%Y-%m-%d'),
                    'symbol': ticker,
                    'revenue': float(revenue) if revenue and not pd.isna(revenue) else None,
                    'grossProfit': float(gross_profit) if gross_profit and not pd.isna(gross_profit) else None,
                    'netIncome': float(net_income) if net_income and not pd.isna(net_income) else None,
                    'period': 'Q',
                    'calendarYear': date_col.year
                }
                
                formatted_records.append(record)
                
            except Exception as e:
                logger.warning(f"Error formatting yfinance data for {date_col}: {e}")
                continue
        
        return formatted_records


def extract_all_companies(tickers: List[str] = None) -> Dict[str, Dict]:
    """Extract financial data for all target companies."""
    if tickers is None:
        tickers = ['TSLA', 'RIVN', 'LCID']
    
    results = {}
    fmp_extractor = FMPExtractor()
    yf_extractor = YFinanceExtractor()
    
    for ticker in tickers:
        logger.info(f"Processing {ticker}...")
        results[ticker] = {
            'income_data': None,
            'estimates_data': None,
            'status': 'pending',
            'source': None,
            'errors': []
        }
        
        try:
            # Try FMP first
            try:
                income_data = fmp_extractor.get_quarterly_income_statement(ticker)
                estimates_data = fmp_extractor.get_analyst_estimates(ticker)
                
                results[ticker].update({
                    'income_data': income_data,
                    'estimates_data': estimates_data,
                    'status': 'success',
                    'source': 'fmp'
                })
                
                logger.info(f"Successfully extracted {ticker} data via FMP")
                
            except (RateLimitError, APIError) as e:
                logger.warning(f"FMP failed for {ticker}: {e}. Trying yfinance fallback...")
                
                # Fallback to yfinance
                income_data = yf_extractor.get_quarterly_income_statement(ticker)
                
                results[ticker].update({
                    'income_data': income_data,
                    'estimates_data': {},  # yfinance doesn't provide estimates
                    'status': 'partial',
                    'source': 'yfinance',
                    'errors': [str(e)]
                })
                
                logger.info(f"Partially extracted {ticker} data via yfinance")
        
        except Exception as e:
            logger.error(f"Failed to extract {ticker} data: {e}")
            results[ticker].update({
                'status': 'failed',
                'errors': [str(e)]
            })
        
        # Add delay between requests to be respectful
        time.sleep(1)
    
    # Log summary
    successful = sum(1 for r in results.values() if r['status'] in ['success', 'partial'])
    logger.info(f"Extraction complete: {successful}/{len(tickers)} companies processed successfully")
    
    return results


if __name__ == "__main__":
    # Test the extraction
    results = extract_all_companies()
    print(f"Extraction results: {results}")