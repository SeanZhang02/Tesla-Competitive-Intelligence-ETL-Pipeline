"""
Edge case tests for API failures, missing data, and error scenarios.
Tests resilience and error handling across the ETL pipeline.
"""
import pytest
import json
import os
from unittest.mock import Mock, patch, mock_open
from datetime import date
from decimal import Decimal

import pandas as pd
import requests

from extract import FMPExtractor, YFinanceExtractor, extract_all_companies, RateLimitError, APIError
from transform import DataTransformer, ValidationError
from load import DatabaseLoader, LoadError
from config import FinancialData


class TestAPIFailureScenarios:
    """Test various API failure scenarios."""
    
    @pytest.fixture
    def fmp_extractor(self):
        return FMPExtractor(api_key="test_key", rate_limit=250)
    
    @pytest.fixture
    def yf_extractor(self):
        return YFinanceExtractor()
    
    def test_fmp_authentication_failure(self, fmp_extractor):
        """Test FMP API authentication failure."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "Invalid API key"}
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
        
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    def test_fmp_rate_limit_exceeded(self, fmp_extractor):
        """Test FMP API rate limit handling."""
        fmp_extractor.daily_calls = 250  # At the limit
        
        with pytest.raises(RateLimitError, match="Daily API limit"):
            fmp_extractor._check_rate_limit()
    
    def test_fmp_network_timeout(self, fmp_extractor):
        """Test network timeout scenarios."""
        fmp_extractor.session.get = Mock(side_effect=requests.exceptions.Timeout("Request timed out"))
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    def test_fmp_connection_error(self, fmp_extractor):
        """Test network connection errors."""
        fmp_extractor.session.get = Mock(side_effect=requests.exceptions.ConnectionError("Network unreachable"))
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    def test_fmp_server_error(self, fmp_extractor):
        """Test server-side errors (5xx)."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Internal Server Error")
        
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    @patch('extract.yf.Ticker')
    def test_yfinance_data_access_error(self, mock_ticker, yf_extractor):
        """Test yfinance data access errors."""
        mock_ticker_instance = Mock()
        mock_ticker_instance.quarterly_income_stmt = Mock(side_effect=Exception("Data not available"))
        mock_ticker.return_value = mock_ticker_instance
        
        with pytest.raises(APIError, match="yfinance extraction failed"):
            yf_extractor.get_quarterly_income_statement("INVALID_TICKER")
    
    @patch('extract.yf.Ticker')
    def test_yfinance_empty_dataframe(self, mock_ticker, yf_extractor):
        """Test yfinance returning empty DataFrame."""
        mock_ticker_instance = Mock()
        mock_ticker_instance.quarterly_income_stmt = pd.DataFrame()  # Empty DataFrame
        mock_ticker.return_value = mock_ticker_instance
        
        result = yf_extractor.get_quarterly_income_statement("TSLA")
        assert result == {}


class TestMissingDataScenarios:
    """Test scenarios with missing or incomplete data."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    @pytest.fixture
    def fmp_extractor(self):
        return FMPExtractor(api_key="test_key")
    
    def test_fmp_empty_response(self, fmp_extractor):
        """Test FMP API returning empty response."""
        mock_response = Mock()
        mock_response.json.return_value = []  # Empty list
        mock_response.raise_for_status.return_value = None
        
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        # Should handle empty response gracefully
        result = fmp_extractor.get_quarterly_income_statement("UNKNOWN_TICKER")
        assert result == []
    
    def test_fmp_malformed_json(self, fmp_extractor):
        """Test FMP API returning malformed JSON."""
        mock_response = Mock()
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.raise_for_status.return_value = None
        
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    def test_missing_required_fields(self, transformer):
        """Test transformation with missing required fields."""
        incomplete_data = [
            {
                "date": "2025-06-30",
                "symbol": "TSLA",
                # Missing revenue, eps, grossProfit
            }
        ]
        
        result = transformer.extract_core_metrics(incomplete_data, "TSLA", "fmp")
        
        # Should still create FinancialData objects but with None values
        assert len(result) == 1
        assert result[0].revenue is None
        assert result[0].eps is None
        assert result[0].gross_profit is None
    
    def test_invalid_date_formats(self, transformer):
        """Test handling of invalid date formats."""
        invalid_date_data = [
            {
                "date": "invalid-date",
                "symbol": "TSLA",
                "revenue": 22500000000,
                "eps": 0.40,
                "grossProfit": 5000000000
            }
        ]
        
        result = transformer.extract_core_metrics(invalid_date_data, "TSLA", "fmp")
        
        # Should handle invalid dates gracefully
        assert len(result) == 1
        assert result[0].quarter_date is None
        assert result[0].quarter_label is None
    
    def test_non_numeric_financial_values(self, transformer):
        """Test handling of non-numeric financial values."""
        non_numeric_data = [
            {
                "date": "2025-06-30",
                "symbol": "TSLA",
                "revenue": "N/A",
                "eps": "null",
                "grossProfit": "TBD"
            }
        ]
        
        result = transformer.extract_core_metrics(non_numeric_data, "TSLA", "fmp")
        
        # Should convert gracefully to None
        assert len(result) == 1
        assert result[0].revenue is None
        assert result[0].eps is None
        assert result[0].gross_profit is None
    
    def test_extreme_financial_values(self, transformer):
        """Test handling of extreme financial values.""" 
        extreme_data = [
            {
                "date": "2025-06-30",
                "symbol": "TSLA",
                "revenue": 999999999999999,  # Extremely large
                "eps": -999.99,             # Extremely negative
                "grossProfit": 0.000001     # Extremely small
            }
        ]
        
        result = transformer.extract_core_metrics(extreme_data, "TSLA", "fmp")
        
        # Should handle extreme values without error
        assert len(result) == 1
        assert result[0].revenue is not None
        assert result[0].eps is not None
        assert result[0].gross_profit is not None


class TestFileSystemEdgeCases:
    """Test file system related edge cases."""
    
    @pytest.fixture
    def fmp_extractor(self):
        return FMPExtractor(api_key="test_key")
    
    @patch('extract.os.makedirs')
    def test_directory_creation_permission_error(self, mock_makedirs, fmp_extractor):
        """Test handling of directory creation permission errors."""
        mock_makedirs.side_effect = PermissionError("Permission denied")
        
        mock_response = Mock()
        mock_response.json.return_value = [{"test": "data"}]
        mock_response.raise_for_status.return_value = None
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with pytest.raises(APIError, match="Failed to fetch data"):
            fmp_extractor.get_quarterly_income_statement("TSLA")
    
    @patch('builtins.open')
    def test_file_write_permission_error(self, mock_open_func, fmp_extractor):
        """Test handling of file write permission errors."""
        mock_open_func.side_effect = PermissionError("Permission denied")
        
        mock_response = Mock()
        mock_response.json.return_value = [{"test": "data"}]
        mock_response.raise_for_status.return_value = None
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with patch('extract.os.makedirs'):  # Allow directory creation
            with pytest.raises(APIError, match="Failed to fetch data"):
                fmp_extractor.get_quarterly_income_statement("TSLA")
    
    @patch('builtins.open')
    def test_disk_full_error(self, mock_open_func, fmp_extractor):
        """Test handling of disk full errors."""
        mock_open_func.side_effect = OSError("No space left on device")
        
        mock_response = Mock()
        mock_response.json.return_value = [{"test": "data"}]
        mock_response.raise_for_status.return_value = None
        fmp_extractor.session.get = Mock(return_value=mock_response)
        
        with patch('extract.os.makedirs'):
            with pytest.raises(APIError, match="Failed to fetch data"):
                fmp_extractor.get_quarterly_income_statement("TSLA")


class TestDatabaseEdgeCases:
    """Test database-related edge cases."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    def test_database_connection_timeout(self, loader):
        """Test database connection timeout."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        mock_session.execute.side_effect = Exception("Connection timeout")
        
        loader.get_session = Mock(return_value=mock_session)
        
        with pytest.raises(LoadError, match="Company loading failed"):
            loader.load_companies(['TSLA'])
    
    def test_database_transaction_rollback(self, loader):
        """Test database transaction rollback on error."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        mock_session.commit.side_effect = Exception("Commit failed")
        
        loader.session_factory = Mock(return_value=mock_session)
        
        with pytest.raises(Exception, match="Commit failed"):
            with loader.get_session() as session:
                pass  # Trigger commit in context manager
        
        mock_session.rollback.assert_called_once()
    
    def test_invalid_company_mapping(self, loader):
        """Test handling of invalid company mappings."""
        financial_data = [FinancialData(
            ticker='UNKNOWN_COMPANY',
            quarter_date=date(2025, 6, 30),
            quarter_label='2025-Q2',
            revenue=Decimal('1000000000'),
            eps=Decimal('0.10'),
            gross_profit=Decimal('200000000')
        )]
        
        loader.company_cache = {}  # Empty cache
        loader.load_companies = Mock(return_value={})  # No companies loaded
        
        result = loader.load_quarterly_financials(financial_data)
        
        # Should handle missing company gracefully
        assert result == 0


class TestDataValidationEdgeCases:
    """Test data validation edge cases."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    def test_tesla_validation_with_null_values(self, transformer):
        """Test Tesla validation with null financial values."""
        tesla_data_with_nulls = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=None,  # Null revenue
            eps=None,      # Null EPS
            gross_profit=Decimal('5000000000')
        )]
        
        # Should fail validation due to null values
        with pytest.raises(ValidationError, match="Tesla Q2 2025 revenue validation failed"):
            transformer.validate_tesla_q2_2025(tesla_data_with_nulls)
    
    def test_tesla_validation_precision_issues(self, transformer):
        """Test Tesla validation with floating point precision issues."""
        tesla_data_precision = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=Decimal('22500000000.01'),  # Slightly off due to precision
            eps=Decimal('0.399999999'),         # Slightly off due to precision
            gross_profit=Decimal('5000000000')
        )]
        
        # Should still pass validation within tolerance
        result = transformer.validate_tesla_q2_2025(tesla_data_precision)
        assert result == True
    
    def test_quarter_standardization_edge_cases(self, transformer):
        """Test quarter standardization with edge cases."""
        # Test various edge cases for date parsing
        test_cases = [
            ("", None),
            (None, None),
            ("2025-13-31", None),    # Invalid month
            ("2025-02-30", None),    # Invalid date
            ("not-a-date", None),    # Invalid format
            ("2025/12/31", "2025-Q4"),  # Different format should work
        ]
        
        for input_date, expected in test_cases:
            result = transformer.standardize_quarter_date(input_date)
            assert result == expected
    
    def test_decimal_conversion_edge_cases(self, transformer):
        """Test decimal conversion with various edge cases."""
        test_cases = [
            (float('inf'), None),     # Infinity
            (float('-inf'), None),    # Negative infinity
            (float('nan'), None),     # NaN
            ("", None),               # Empty string
            ("invalid", None),        # Invalid string
            (None, None),             # None input
            (0, Decimal('0')),        # Zero
            (-1000000, Decimal('-1000000')),  # Negative
        ]
        
        for input_val, expected in test_cases:
            result = transformer._safe_decimal_convert(input_val)
            assert result == expected


class TestOrchestrationEdgeCases:
    """Test orchestration and workflow edge cases."""
    
    @patch('extract.FMPExtractor')
    @patch('extract.YFinanceExtractor')
    def test_all_extractors_fail(self, mock_yf, mock_fmp):
        """Test scenario where all extractors fail."""
        # Mock both extractors failing
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_quarterly_income_statement.side_effect = RateLimitError("Rate limited")
        mock_fmp_instance.get_analyst_estimates.side_effect = RateLimitError("Rate limited") 
        mock_fmp.return_value = mock_fmp_instance
        
        mock_yf_instance = Mock()
        mock_yf_instance.get_quarterly_income_statement.side_effect = APIError("yfinance failed")
        mock_yf.return_value = mock_yf_instance
        
        result = extract_all_companies(['TSLA'])
        
        # Should return failed status for all companies
        assert result['TSLA']['status'] == 'failed'
        assert len(result['TSLA']['errors']) > 0
    
    @patch('extract.FMPExtractor')
    @patch('extract.YFinanceExtractor')
    @patch('extract.time.sleep')  # Mock sleep to speed up tests
    def test_retry_logic_exhaustion(self, mock_sleep, mock_yf, mock_fmp):
        """Test that retry logic eventually gives up."""
        # Mock FMP failing consistently
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_quarterly_income_statement.side_effect = APIError("Persistent error")
        mock_fmp.return_value = mock_fmp_instance
        
        # Mock yfinance also failing
        mock_yf_instance = Mock()
        mock_yf_instance.get_quarterly_income_statement.side_effect = APIError("Also failing")
        mock_yf.return_value = mock_yf_instance
        
        result = extract_all_companies(['TSLA'])
        
        # Should eventually give up and mark as failed
        assert result['TSLA']['status'] == 'failed'
        # Verify sleep was called (indicating retries happened)
        assert mock_sleep.call_count > 0
    
    def test_memory_pressure_large_dataset(self):
        """Test handling of large datasets that might cause memory pressure."""
        # Create a large amount of mock data
        large_dataset = []
        for i in range(10000):  # 10k records
            large_dataset.append({
                "date": "2025-06-30",
                "symbol": "TSLA",
                "revenue": 22500000000 + i,
                "eps": 0.40,
                "grossProfit": 5000000000 + i
            })
        
        transformer = DataTransformer()
        
        # Should handle large datasets without memory errors
        result = transformer.extract_core_metrics(large_dataset, "TSLA", "fmp")
        
        assert len(result) == 10000
        assert all(isinstance(item, FinancialData) for item in result)


if __name__ == "__main__":
    pytest.main([__file__])