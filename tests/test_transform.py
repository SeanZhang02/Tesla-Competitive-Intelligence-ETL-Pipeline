"""
Unit tests for transform.py module.
Tests data transformation and validation logic.
"""
import pytest
import pandas as pd
from datetime import date, datetime
from decimal import Decimal

from transform import DataTransformer, ValidationError
from config import FinancialData


class TestDataTransformer:
    """Test data transformation functionality."""
    
    @pytest.fixture
    def transformer(self):
        """Create DataTransformer instance for testing."""
        return DataTransformer()
    
    @pytest.fixture
    def sample_fmp_data(self):
        """Sample FMP API response data."""
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
    
    @pytest.fixture
    def tesla_q2_2025_data(self):
        """Tesla Q2 2025 validation data."""
        return [
            FinancialData(
                ticker='TSLA',
                quarter_date=date(2025, 6, 30),
                quarter_label='2025-Q2',
                revenue=Decimal('22500000000'),  # $22.5B
                eps=Decimal('0.40'),
                gross_profit=Decimal('5000000000')
            )
        ]


class TestQuarterStandardization:
    """Test quarter date standardization functionality."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    def test_quarter_standardization(self, transformer):
        """Test quarter date standardization as specified in PRP."""
        assert transformer.standardize_quarter_date("2025-06-30") == "2025-Q2"
        assert transformer.standardize_quarter_date("2025-03-31") == "2025-Q1"
        assert transformer.standardize_quarter_date("2025-09-30") == "2025-Q3"
        assert transformer.standardize_quarter_date("2025-12-31") == "2025-Q4"
    
    def test_quarter_standardization_edge_cases(self, transformer):
        """Test quarter standardization with edge cases."""
        # Test with datetime objects
        assert transformer.standardize_quarter_date(datetime(2025, 6, 30)) == "2025-Q2"
        assert transformer.standardize_quarter_date(date(2025, 3, 31)) == "2025-Q1"
        
        # Test with None and invalid inputs
        assert transformer.standardize_quarter_date(None) is None
        assert transformer.standardize_quarter_date("") is None
        
        # Test with different date formats
        assert transformer.standardize_quarter_date("06/30/2025") == "2025-Q2"
    
    def test_quarter_boundary_dates(self, transformer):
        """Test quarter boundary dates."""
        # Q1 boundaries
        assert transformer.standardize_quarter_date("2025-01-01") == "2025-Q1"
        assert transformer.standardize_quarter_date("2025-03-31") == "2025-Q1"
        
        # Q2 boundaries
        assert transformer.standardize_quarter_date("2025-04-01") == "2025-Q2"
        assert transformer.standardize_quarter_date("2025-06-30") == "2025-Q2"
        
        # Q3 boundaries
        assert transformer.standardize_quarter_date("2025-07-01") == "2025-Q3"
        assert transformer.standardize_quarter_date("2025-09-30") == "2025-Q3"
        
        # Q4 boundaries
        assert transformer.standardize_quarter_date("2025-10-01") == "2025-Q4"
        assert transformer.standardize_quarter_date("2025-12-31") == "2025-Q4"


class TestTeslaValidation:
    """Test Tesla Q2 2025 validation as specified in PRP."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    def test_tesla_q2_2025_validation_success(self, transformer):
        """Ensure Tesla Q2 2025 data validates correctly."""
        # Create test data with exact expected values
        data = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=Decimal('22500000000'),  # $22.5B
            eps=Decimal('0.40'),
            gross_profit=Decimal('5000000000')
        )]
        
        # Should pass validation
        assert transformer.validate_tesla_q2_2025(data) == True
    
    def test_tesla_q2_2025_validation_revenue_failure(self, transformer):
        """Test Tesla validation fails with wrong revenue."""
        data = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=Decimal('20000000000'),  # Wrong revenue
            eps=Decimal('0.40'),
            gross_profit=Decimal('5000000000')
        )]
        
        with pytest.raises(ValidationError, match="Tesla Q2 2025 revenue validation failed"):
            transformer.validate_tesla_q2_2025(data)
    
    def test_tesla_q2_2025_validation_eps_failure(self, transformer):
        """Test Tesla validation fails with wrong EPS."""
        data = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=Decimal('22500000000'),
            eps=Decimal('0.30'),  # Wrong EPS
            gross_profit=Decimal('5000000000')
        )]
        
        with pytest.raises(ValidationError, match="Tesla Q2 2025 EPS validation failed"):
            transformer.validate_tesla_q2_2025(data)
    
    def test_tesla_q2_2025_missing_data(self, transformer):
        """Test Tesla validation when Q2 2025 data is missing."""
        # Data without Tesla Q2 2025
        data = [FinancialData(
            ticker='TSLA',
            quarter_label='2025-Q1',
            quarter_date=date(2025, 3, 31),
            revenue=Decimal('20000000000'),
            eps=Decimal('0.35'),
            gross_profit=Decimal('4500000000')
        )]
        
        # Should return False but not raise exception
        assert transformer.validate_tesla_q2_2025(data) == False


class TestDataTransformation:
    """Test core data transformation functionality."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    def test_safe_decimal_convert(self, transformer):
        """Test safe decimal conversion."""
        # Valid conversions
        assert transformer._safe_decimal_convert("100") == Decimal('100')
        assert transformer._safe_decimal_convert(100) == Decimal('100')
        assert transformer._safe_decimal_convert("1000000") == Decimal('1000000')
        
        # Million conversion (values < 1M get multiplied)
        assert transformer._safe_decimal_convert("100") == Decimal('100000000')  # 100 * 1M
        assert transformer._safe_decimal_convert(22.5) == Decimal('22500000')    # 22.5 * 1M
        
        # Invalid/None conversions
        assert transformer._safe_decimal_convert(None) is None
        assert transformer._safe_decimal_convert("") is None
        assert transformer._safe_decimal_convert("N/A") is None
        assert transformer._safe_decimal_convert("invalid") is None
    
    def test_extract_core_metrics_fmp(self, transformer, sample_fmp_data):
        """Test extracting core metrics from FMP data."""
        results = transformer.extract_core_metrics(sample_fmp_data, "TSLA", "fmp")
        
        assert len(results) == 2
        assert all(isinstance(r, FinancialData) for r in results)
        assert results[0].ticker == "TSLA"
        assert results[0].quarter_label == "2025-Q2"
        assert results[0].revenue == Decimal('22500000000')
    
    def test_extract_core_metrics_empty_data(self, transformer):
        """Test extraction with empty data."""
        results = transformer.extract_core_metrics({}, "TSLA", "fmp")
        assert results == []
        
        results = transformer.extract_core_metrics(None, "TSLA", "fmp")
        assert results == []
    
    def test_to_dataframe_conversion(self, transformer):
        """Test conversion to pandas DataFrame."""
        financial_data = [FinancialData(
            ticker='TSLA',
            quarter_date=date(2025, 6, 30),
            quarter_label='2025-Q2',
            revenue=Decimal('22500000000'),
            eps=Decimal('0.40'),
            gross_profit=Decimal('5000000000')
        )]
        
        df = transformer.to_dataframe(financial_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['ticker'] == 'TSLA'
        assert df.iloc[0]['quarter_label'] == '2025-Q2'
        assert df.iloc[0]['revenue'] == 22500000000.0
        assert df.iloc[0]['eps'] == 0.40
    
    def test_to_dataframe_empty(self, transformer):
        """Test DataFrame conversion with empty data."""
        df = transformer.to_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestTransformationWorkflow:
    """Test the complete transformation workflow."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()
    
    @pytest.fixture
    def extraction_results(self):
        """Mock extraction results."""
        return {
            'TSLA': {
                'status': 'success',
                'source': 'fmp',
                'income_data': [
                    {
                        "date": "2025-06-30",
                        "symbol": "TSLA",
                        "revenue": 22500000000,
                        "eps": 0.40,
                        "grossProfit": 5000000000
                    }
                ]
            },
            'RIVN': {
                'status': 'failed',
                'errors': ['API Error']
            }
        }
    
    def test_transform_all_data(self, transformer, extraction_results):
        """Test transforming all extraction results."""
        results = transformer.transform_all_data(extraction_results)
        
        # Should only process successful extractions
        assert len(results) == 1
        assert results[0].ticker == 'TSLA'
        assert results[0].quarter_label == '2025-Q2'
    
    def test_transform_all_data_empty_results(self, transformer):
        """Test transformation with empty extraction results."""
        results = transformer.transform_all_data({})
        assert results == []


if __name__ == "__main__":
    pytest.main([__file__])