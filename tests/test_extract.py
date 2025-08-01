"""
Unit tests for extract.py module.
Tests API extraction functionality with mocked responses.
"""
import json
import pytest
from unittest.mock import Mock, patch, mock_open
from decimal import Decimal

from extract import FMPExtractor, YFinanceExtractor, extract_all_companies, RateLimitError, APIError


class TestFMPExtractor:
    """Test Financial Modeling Prep API extractor."""
    
    @pytest.fixture
    def extractor(self):
        """Create FMPExtractor instance for testing."""
        return FMPExtractor(api_key="test_key", rate_limit=10)
    
    @pytest.fixture
    def mock_response_data(self):
        """Mock API response data."""
        return [
            {
                "date": "2025-06-30",
                "symbol": "TSLA",
                "revenue": 22500000000,
                "eps": 0.40,
                "grossProfit": 5000000000
            },
            {
                "date": "2025-03-31", 
                "symbol": "TSLA",
                "revenue": 20000000000,
                "eps": 0.35,
                "grossProfit": 4500000000
            }
        ]
    
    def test_rate_limit_check(self, extractor):
        """Test rate limit checking functionality."""
        extractor.daily_calls = 15  # Exceed the limit of 10
        
        with pytest.raises(RateLimitError, match="Daily API limit"):
            extractor._check_rate_limit()
    
    @patch('extract.os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('extract.json.dump')
    def test_get_quarterly_income_statement_success(self, mock_json_dump, mock_file, mock_makedirs, extractor, mock_response_data):
        """Test successful quarterly income statement extraction."""
        # Mock the session.get call
        mock_response = Mock()
        mock_response.json.return_value = mock_response_data
        mock_response.raise_for_status.return_value = None
        
        extractor.session.get = Mock(return_value=mock_response)
        
        result = extractor.get_quarterly_income_statement("TSLA")
        
        # Assertions
        assert result == mock_response_data
        assert extractor.daily_calls == 1
        mock_makedirs.assert_called_once_with('data/raw', exist_ok=True)
        mock_file.assert_called_once_with('data/raw/TSLA_income_raw.json', 'w')
        mock_json_dump.assert_called_once()
    
    @patch('extract.requests.exceptions.RequestException')
    def test_api_request_failure(self, mock_exception, extractor):
        """Test API request failure handling."""
        extractor.session.get = Mock(side_effect=mock_exception("Network error"))
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            extractor.get_quarterly_income_statement("TSLA")


class TestYFinanceExtractor:
    """Test yfinance fallback extractor."""
    
    @pytest.fixture
    def extractor(self):
        """Create YFinanceExtractor instance for testing."""
        return YFinanceExtractor()
    
    @pytest.fixture
    def mock_yf_data(self):
        """Mock yfinance quarterly data."""
        import pandas as pd
        from datetime import date
        
        data = pd.DataFrame({
            date(2025, 6, 30): [22500000000, 5000000000, 1000000000],
            date(2025, 3, 31): [20000000000, 4500000000, 900000000]
        }, index=['Total Revenue', 'Gross Profit', 'Net Income'])
        
        return data
    
    @patch('extract.os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('extract.json.dump')
    @patch('extract.yf.Ticker')
    def test_yfinance_extraction_success(self, mock_ticker, mock_json_dump, mock_file, mock_makedirs, extractor, mock_yf_data):
        """Test successful yfinance data extraction."""
        # Mock yfinance ticker
        mock_ticker_instance = Mock()
        mock_ticker_instance.quarterly_income_stmt = mock_yf_data
        mock_ticker.return_value = mock_ticker_instance
        
        result = extractor.get_quarterly_income_statement("TSLA")
        
        # Assertions
        assert isinstance(result, list)
        assert len(result) > 0
        assert result[0]['symbol'] == 'TSLA'
        mock_makedirs.assert_called_once_with('data/raw', exist_ok=True)
    
    @patch('extract.yf.Ticker')
    def test_yfinance_empty_data(self, mock_ticker, extractor):
        """Test yfinance extraction with empty data."""
        import pandas as pd
        
        # Mock empty DataFrame
        mock_ticker_instance = Mock()
        mock_ticker_instance.quarterly_income_stmt = pd.DataFrame()
        mock_ticker.return_value = mock_ticker_instance
        
        result = extractor.get_quarterly_income_statement("TSLA")
        
        assert result == {}


class TestExtractAllCompanies:
    """Test the main extraction orchestration function."""
    
    @patch('extract.YFinanceExtractor')
    @patch('extract.FMPExtractor')
    def test_extract_all_companies_success(self, mock_fmp, mock_yf):
        """Test successful extraction for all companies."""
        # Mock FMP extractor
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_quarterly_income_statement.return_value = {"data": "income"}
        mock_fmp_instance.get_analyst_estimates.return_value = {"data": "estimates"}
        mock_fmp.return_value = mock_fmp_instance
        
        result = extract_all_companies(['TSLA'])
        
        # Assertions
        assert 'TSLA' in result
        assert result['TSLA']['status'] == 'success'
        assert result['TSLA']['source'] == 'fmp'
        assert result['TSLA']['income_data'] == {"data": "income"}
        assert result['TSLA']['estimates_data'] == {"data": "estimates"}
    
    @patch('extract.YFinanceExtractor')
    @patch('extract.FMPExtractor')
    def test_extract_with_fmp_fallback_to_yfinance(self, mock_fmp, mock_yf):
        """Test fallback to yfinance when FMP fails."""
        # Mock FMP failure
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_quarterly_income_statement.side_effect = RateLimitError("Rate limit exceeded")
        mock_fmp.return_value = mock_fmp_instance
        
        # Mock yfinance success
        mock_yf_instance = Mock()
        mock_yf_instance.get_quarterly_income_statement.return_value = {"data": "yfinance"}
        mock_yf.return_value = mock_yf_instance
        
        result = extract_all_companies(['TSLA'])
        
        # Assertions
        assert result['TSLA']['status'] == 'partial'
        assert result['TSLA']['source'] == 'yfinance'
        assert result['TSLA']['income_data'] == {"data": "yfinance"}
    
    @patch('extract.time.sleep')  # Mock sleep to speed up tests
    @patch('extract.YFinanceExtractor')
    @patch('extract.FMPExtractor')
    def test_extract_complete_failure(self, mock_fmp, mock_yf, mock_sleep):
        """Test complete extraction failure for a company."""
        # Mock both extractors failing
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_quarterly_income_statement.side_effect = APIError("API Error")
        mock_fmp.return_value = mock_fmp_instance
        
        mock_yf_instance = Mock()
        mock_yf_instance.get_quarterly_income_statement.side_effect = APIError("yfinance Error")
        mock_yf.return_value = mock_yf_instance
        
        result = extract_all_companies(['TSLA'])
        
        # Assertions
        assert result['TSLA']['status'] == 'failed'
        assert len(result['TSLA']['errors']) > 0


if __name__ == "__main__":
    pytest.main([__file__])