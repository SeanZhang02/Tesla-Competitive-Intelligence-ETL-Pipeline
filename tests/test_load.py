"""
Unit tests for load.py module.
Tests database loading operations with mocked database.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from decimal import Decimal

from load import DatabaseLoader, LoadError
from config import FinancialData, Company, QuarterlyFinancial


class TestDatabaseLoader:
    """Test database loading functionality."""
    
    @pytest.fixture
    def loader(self):
        """Create DatabaseLoader instance with mocked session factory."""
        with patch('load.get_session_factory') as mock_factory:
            loader = DatabaseLoader()
            loader.session_factory = mock_factory
            return loader
    
    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        session.close = Mock()
        session.execute = Mock()
        return session
    
    @pytest.fixture
    def sample_financial_data(self):
        """Sample financial data for testing."""
        return [
            FinancialData(
                ticker='TSLA',
                quarter_date=date(2025, 6, 30),
                quarter_label='2025-Q2',
                revenue=Decimal('22500000000'),
                eps=Decimal('0.40'),
                gross_profit=Decimal('5000000000')
            ),
            FinancialData(
                ticker='RIVN',
                quarter_date=date(2025, 6, 30),
                quarter_label='2025-Q2',
                revenue=Decimal('1500000000'),
                eps=Decimal('-0.50'),
                gross_profit=Decimal('300000000')
            )
        ]


class TestCompanyLoading:
    """Test company loading functionality."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    def test_load_companies_success(self, loader):
        """Test successful company loading."""
        # Mock session and database results
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock existing companies query (empty result)
        mock_session.execute.return_value.fetchall.return_value = []
        
        # Mock new companies insert and final query
        existing_companies = [
            Mock(Company=Mock(ticker='TSLA', id=1)),
            Mock(Company=Mock(ticker='RIVN', id=2)),
            Mock(Company=Mock(ticker='LCID', id=3))
        ]
        mock_session.execute.return_value.fetchall.side_effect = [[], existing_companies]
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.load_companies(['TSLA', 'RIVN', 'LCID'])
        
        # Assertions
        assert result == {'TSLA': 1, 'RIVN': 2, 'LCID': 3}
        assert loader.company_cache == {'TSLA': 1, 'RIVN': 2, 'LCID': 3}
    
    def test_load_companies_with_existing(self, loader):
        """Test loading companies when some already exist."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock existing Tesla
        existing_tesla = [Mock(Company=Mock(ticker='TSLA', id=1))]
        all_companies = [
            Mock(Company=Mock(ticker='TSLA', id=1)),
            Mock(Company=Mock(ticker='RIVN', id=2))
        ]
        
        mock_session.execute.return_value.fetchall.side_effect = [existing_tesla, all_companies]
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.load_companies(['TSLA', 'RIVN'])
        
        assert 'TSLA' in result
        assert 'RIVN' in result


class TestFinancialDataLoading:
    """Test financial data loading functionality."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            loader = DatabaseLoader()
            loader.company_cache = {'TSLA': 1, 'RIVN': 2}
            return loader
    
    def test_load_quarterly_financials_success(self, loader, sample_financial_data):
        """Test successful financial data loading."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock successful bulk insert
        mock_session.execute.return_value = Mock()
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.load_quarterly_financials(sample_financial_data)
        
        assert result == 2  # Two records loaded
        mock_session.execute.assert_called()
    
    def test_load_quarterly_financials_empty_data(self, loader):
        """Test loading with empty financial data."""
        result = loader.load_quarterly_financials([])
        assert result == 0
    
    def test_load_quarterly_financials_missing_company(self, loader):
        """Test loading financial data for unknown company."""
        unknown_data = [FinancialData(
            ticker='UNKNOWN',
            quarter_date=date(2025, 6, 30),
            quarter_label='2025-Q2',
            revenue=Decimal('1000000000'),
            eps=Decimal('0.10'),
            gross_profit=Decimal('200000000')
        )]
        
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        loader.get_session = Mock(return_value=mock_session)
        
        # Should load companies first, then proceed
        loader.load_companies = Mock(return_value={'UNKNOWN': 3})
        
        result = loader.load_quarterly_financials(unknown_data)
        
        # Should still work after loading companies
        loader.load_companies.assert_called_once()
    
    @patch('load.IntegrityError')
    def test_load_quarterly_financials_duplicate_handling(self, mock_integrity_error, loader, sample_financial_data):
        """Test handling of duplicate financial records."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock IntegrityError on first insert, then successful upsert
        mock_session.execute.side_effect = [
            mock_integrity_error("Duplicate key"),
            Mock(),  # Successful select for existing record
            Mock(),  # Successful update
        ]
        
        # Mock existing record query
        mock_session.execute.return_value.first.return_value = Mock(
            QuarterlyFinancial=Mock(id=1)
        )
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.load_quarterly_financials(sample_financial_data)
        
        # Should handle duplicates gracefully
        assert result >= 0


class TestDataFrameLoading:
    """Test DataFrame loading functionality."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame([
            {
                'ticker': 'TSLA',
                'quarter_date': '2025-06-30',
                'quarter_label': '2025-Q2',
                'revenue': 22500000000.0,
                'eps': 0.40,
                'gross_profit': 5000000000.0
            }
        ])
    
    def test_load_from_dataframe_success(self, loader, sample_dataframe):
        """Test successful DataFrame loading."""
        # Mock the load_quarterly_financials method
        loader.load_quarterly_financials = Mock(return_value=1)
        
        result = loader.load_from_dataframe(sample_dataframe)
        
        assert result == 1
        loader.load_quarterly_financials.assert_called_once()
    
    def test_load_from_dataframe_empty(self, loader):
        """Test loading empty DataFrame."""
        empty_df = pd.DataFrame()
        result = loader.load_from_dataframe(empty_df)
        assert result == 0


class TestDatabaseValidation:
    """Test database validation functionality."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    def test_validate_tesla_data_success(self, loader):
        """Test successful Tesla Q2 2025 validation in database."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock Tesla company query
        tesla_company = Mock(Company=Mock(id=1))
        mock_session.execute.return_value.first.side_effect = [
            tesla_company,  # Tesla company exists
            Mock(QuarterlyFinancial=Mock(  # Q2 2025 data with correct values
                revenue=22500000000.0,
                eps=0.40
            ))
        ]
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.validate_tesla_data()
        
        assert result == True
    
    def test_validate_tesla_data_missing_company(self, loader):
        """Test validation when Tesla company is missing."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock Tesla company not found
        mock_session.execute.return_value.first.return_value = None
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.validate_tesla_data()
        
        assert result == False
    
    def test_validate_tesla_data_wrong_values(self, loader):
        """Test validation with incorrect Tesla values."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock Tesla company and wrong data
        tesla_company = Mock(Company=Mock(id=1))
        mock_session.execute.return_value.first.side_effect = [
            tesla_company,
            Mock(QuarterlyFinancial=Mock(  # Wrong values
                revenue=20000000000.0,  # Should be 22.5B
                eps=0.30  # Should be 0.40
            ))
        ]
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.validate_tesla_data()
        
        assert result == False


class TestDataSummary:
    """Test data summary functionality."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    def test_get_data_summary_success(self, loader):
        """Test successful data summary generation."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock companies and financial records
        companies = [
            Mock(Company=Mock(ticker='TSLA', id=1)),
            Mock(Company=Mock(ticker='RIVN', id=2))
        ]
        
        financials_tsla = [Mock(), Mock()]  # 2 records for TSLA
        financials_rivn = [Mock()]  # 1 record for RIVN
        
        mock_session.execute.return_value.fetchall.side_effect = [
            companies,      # Companies query
            financials_tsla,  # TSLA financials
            financials_rivn   # RIVN financials
        ]
        
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.get_data_summary()
        
        assert result['total_companies'] == 2
        assert 'company_breakdown' in result
        assert result['company_breakdown']['TSLA']['financial_records'] == 2
        assert result['company_breakdown']['RIVN']['financial_records'] == 1
    
    def test_get_data_summary_error(self, loader):
        """Test data summary with database error."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)
        
        # Mock database error
        mock_session.execute.side_effect = Exception("Database error")
        loader.get_session = Mock(return_value=mock_session)
        
        result = loader.get_data_summary()
        
        assert 'error' in result
        assert 'Database error' in result['error']


class TestErrorHandling:
    """Test error handling in database operations."""
    
    @pytest.fixture
    def loader(self):
        with patch('load.get_session_factory'):
            return DatabaseLoader()
    
    def test_session_context_manager_error_handling(self, loader):
        """Test that session context manager handles errors properly."""
        mock_session = Mock()
        mock_session.commit.side_effect = Exception("Database error")
        
        loader.session_factory = Mock(return_value=mock_session)
        
        with pytest.raises(Exception, match="Database error"):
            with loader.get_session() as session:
                # This should trigger the exception
                pass
        
        # Should have called rollback
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])