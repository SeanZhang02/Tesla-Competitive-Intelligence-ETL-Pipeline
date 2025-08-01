"""Test suite for dashboard functionality."""
import pytest
import pandas as pd
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock

from dashboard_service import DashboardDataService
from config import Company, QuarterlyFinancial


class TestDashboardDataService:
    """Test cases for DashboardDataService class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=None)
        return session
    
    @pytest.fixture
    def dashboard_service(self, mock_session):
        """Create a DashboardDataService instance with mocked session."""
        with patch('dashboard_service.get_session_factory') as mock_factory:
            mock_factory.return_value = Mock(return_value=mock_session)
            return DashboardDataService()
    
    def test_get_companies_data(self, dashboard_service, mock_session):
        """Test getting companies data."""
        # Mock company data
        mock_companies = [
            Mock(id=1, ticker='TSLA', name='Tesla Inc', sector='Electric Vehicles'),
            Mock(id=2, ticker='RIVN', name='Rivian Automotive Inc', sector='Electric Vehicles'),
            Mock(id=3, ticker='LCID', name='Lucid Group Inc', sector='Electric Vehicles')
        ]
        
        mock_session.query.return_value.filter.return_value.all.return_value = mock_companies
        
        result = dashboard_service.get_companies_data()
        
        assert len(result) == 3
        assert 'TSLA' in result
        assert result['TSLA']['name'] == 'Tesla Inc'
        assert result['TSLA']['id'] == 1
    
    def test_get_quarterly_financials(self, dashboard_service, mock_session):
        """Test getting quarterly financial data."""
        # Mock financial data
        mock_financial_data = [
            Mock(
                ticker='TSLA',
                name='Tesla Inc',
                quarter_date=date(2025, 6, 30),
                quarter_label='2025-Q2',
                revenue=Decimal('22500000000'),
                eps=Decimal('0.37'),
                gross_profit=Decimal('5000000000'),
                updated_at=datetime.now()
            ),
            Mock(
                ticker='RIVN',
                name='Rivian Automotive Inc',
                quarter_date=date(2025, 6, 30),
                quarter_label='2025-Q2',
                revenue=Decimal('1500000000'),
                eps=Decimal('-0.50'),
                gross_profit=Decimal('300000000'),
                updated_at=datetime.now()
            )
        ]
        
        mock_session.query.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = mock_financial_data
        
        result = dashboard_service.get_quarterly_financials()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ticker' in result.columns
        assert 'revenue' in result.columns
        assert result.iloc[0]['ticker'] == 'TSLA'
        assert result.iloc[0]['revenue'] == 22500000000.0
    
    def test_get_performance_metrics(self, dashboard_service, mock_session):
        """Test getting performance metrics for a company."""
        # Mock latest data
        mock_latest = Mock(
            quarter_label='2025-Q2',
            quarter_date=date(2025, 6, 30),
            revenue=Decimal('22500000000'),
            eps=Decimal('0.37'),
            gross_profit=Decimal('5000000000')
        )
        
        # Mock previous data
        mock_previous = Mock(
            quarter_date=date(2025, 3, 31),
            revenue=Decimal('21000000000'),
            eps=Decimal('0.30'),
            gross_profit=Decimal('4500000000')
        )
        
        # Setup mock to return latest first, then previous
        mock_session.query.return_value.join.return_value.filter.return_value.order_by.return_value.first.side_effect = [
            mock_latest, mock_previous
        ]
        
        result = dashboard_service.get_performance_metrics('TSLA')
        
        assert result['latest_quarter'] == '2025-Q2'
        assert result['latest_revenue'] == 22500000000.0
        assert result['latest_eps'] == 0.37
        assert 'revenue_growth' in result
        assert result['revenue_growth'] > 0  # Should be positive growth
    
    def test_get_comparison_data(self, dashboard_service, mock_session):
        """Test getting comparison data across companies."""
        # Mock comparison data
        mock_comparison_data = [
            Mock(
                ticker='TSLA',
                name='Tesla Inc',
                quarter_label='2025-Q2',
                quarter_date=date(2025, 6, 30),
                revenue=Decimal('22500000000')
            ),
            Mock(
                ticker='RIVN',
                name='Rivian Automotive Inc',
                quarter_label='2025-Q2',
                quarter_date=date(2025, 6, 30),
                revenue=Decimal('1500000000')
            )
        ]
        
        # Mock the complex query chain
        mock_subquery = Mock()
        mock_subquery.c.quarter_date = Mock()
        
        mock_session.query.return_value.distinct.return_value.order_by.return_value.limit.return_value.subquery.return_value = mock_subquery
        mock_session.query.return_value.join.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = mock_comparison_data
        
        result = dashboard_service.get_comparison_data('revenue', 8)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ticker' in result.columns
        assert 'value' in result.columns
    
    def test_get_data_freshness(self, dashboard_service, mock_session):
        """Test getting data freshness information."""
        mock_freshness_data = [
            Mock(ticker='TSLA', last_updated=datetime(2025, 8, 1, 10, 0, 0)),
            Mock(ticker='RIVN', last_updated=datetime(2025, 8, 1, 9, 30, 0)),
            Mock(ticker='LCID', last_updated=datetime(2025, 8, 1, 9, 45, 0))
        ]
        
        mock_session.query.return_value.join.return_value.filter.return_value.group_by.return_value.all.return_value = mock_freshness_data
        
        result = dashboard_service.get_data_freshness()
        
        assert len(result) == 3
        assert 'TSLA' in result
        assert isinstance(result['TSLA'], datetime)
    
    def test_health_check_healthy(self, dashboard_service, mock_session):
        """Test health check with healthy system."""
        mock_session.execute.return_value = None
        mock_session.query.return_value.filter.return_value.count.return_value = 3
        mock_session.query.return_value.join.return_value.filter.return_value.count.return_value = 24
        
        result = dashboard_service.health_check()
        
        assert result['status'] == 'healthy'
        assert result['database'] == 'connected'
        assert '3 companies' in result['companies']
        assert '24 records' in result['financial_records']
    
    def test_health_check_unhealthy(self, dashboard_service, mock_session):
        """Test health check with unhealthy system."""
        mock_session.execute.side_effect = Exception("Database connection failed")
        
        result = dashboard_service.health_check()
        
        assert result['status'] == 'unhealthy'
        assert result['database'] == 'connection_failed'
        assert 'error' in result


class TestDashboardCharts:
    """Test cases for dashboard chart functions."""
    
    @pytest.fixture
    def sample_financial_data(self):
        """Create sample financial data for testing."""
        return pd.DataFrame([
            {
                'ticker': 'TSLA',
                'company_name': 'Tesla Inc',
                'quarter_date': date(2025, 6, 30),
                'quarter_label': '2025-Q2',
                'revenue': 22.5,  # In billions
                'eps': 0.37,
                'gross_profit': 5.0
            },
            {
                'ticker': 'RIVN',
                'company_name': 'Rivian Automotive Inc',
                'quarter_date': date(2025, 6, 30),
                'quarter_label': '2025-Q2',
                'revenue': 1.5,
                'eps': -0.50,
                'gross_profit': 0.3
            }
        ])
    
    def test_create_revenue_chart(self, sample_financial_data):
        """Test revenue chart creation."""
        with patch('dashboard.px.line') as mock_line:
            mock_fig = Mock()
            mock_line.return_value = mock_fig
            
            from dashboard import create_revenue_chart
            result = create_revenue_chart(sample_financial_data)
            
            mock_line.assert_called_once()
            call_args = mock_line.call_args
            assert 'revenue' in str(call_args)
            assert 'quarter_date' in str(call_args)
    
    def test_create_revenue_chart_empty_data(self):
        """Test revenue chart creation with empty data."""
        from dashboard import create_revenue_chart
        empty_df = pd.DataFrame()
        
        result = create_revenue_chart(empty_df)
        
        # Should return a figure with annotation for no data
        assert result is not None
    
    def test_create_eps_chart(self, sample_financial_data):
        """Test EPS chart creation."""
        with patch('dashboard.px.line') as mock_line:
            mock_fig = Mock()
            mock_line.return_value = mock_fig
            
            from dashboard import create_eps_chart
            result = create_eps_chart(sample_financial_data)
            
            mock_line.assert_called_once()
            call_args = mock_line.call_args
            assert 'eps' in str(call_args)
    
    def test_create_gross_profit_chart(self, sample_financial_data):
        """Test gross profit chart creation."""
        with patch('dashboard.px.bar') as mock_bar:
            mock_fig = Mock()
            mock_bar.return_value = mock_fig
            
            from dashboard import create_gross_profit_chart
            result = create_gross_profit_chart(sample_financial_data)
            
            mock_bar.assert_called_once()
            call_args = mock_bar.call_args
            assert 'gross_profit' in str(call_args)


class TestDashboardIntegration:
    """Integration tests for dashboard components."""
    
    @patch('dashboard_service.get_session_factory')
    def test_dashboard_service_initialization(self, mock_factory):
        """Test dashboard service can be initialized."""
        mock_session = Mock()
        mock_factory.return_value = Mock(return_value=mock_session)
        
        service = DashboardDataService()
        assert service is not None
        assert service.session_factory is not None
    
    @patch('dashboard.get_dashboard_service')
    @patch('dashboard.st')
    def test_dashboard_main_function_structure(self, mock_st, mock_get_service):
        """Test that main dashboard function has expected structure."""
        # Mock streamlit components
        mock_st.set_page_config = Mock()
        mock_st.title = Mock()
        mock_st.sidebar.header = Mock()
        mock_st.sidebar.multiselect.return_value = ['TSLA', 'RIVN']
        mock_st.sidebar.date_input.return_value = date.today()
        mock_st.sidebar.slider.return_value = 8
        mock_st.sidebar.button.return_value = False
        
        # Mock service
        mock_service = Mock()
        mock_service.get_data_freshness.return_value = {'TSLA': datetime.now()}
        mock_get_service.return_value = mock_service
        
        # Mock load functions to return empty DataFrame
        with patch('dashboard.load_financial_data', return_value=pd.DataFrame()):
            with patch('dashboard.load_performance_metrics', return_value={}):
                try:
                    from dashboard import main
                    main()
                except SystemExit:
                    pass  # Streamlit may call sys.exit
                
                # Verify key components were called
                mock_st.title.assert_called()
                mock_st.sidebar.header.assert_called()


@pytest.fixture
def mock_database_data():
    """Fixture providing mock database data for testing."""
    return {
        'companies': [
            {'id': 1, 'ticker': 'TSLA', 'name': 'Tesla Inc', 'sector': 'Electric Vehicles'},
            {'id': 2, 'ticker': 'RIVN', 'name': 'Rivian Automotive Inc', 'sector': 'Electric Vehicles'},
            {'id': 3, 'ticker': 'LCID', 'name': 'Lucid Group Inc', 'sector': 'Electric Vehicles'}
        ],
        'financials': [
            {
                'company_id': 1, 'ticker': 'TSLA', 'quarter_date': date(2025, 6, 30),
                'quarter_label': '2025-Q2', 'revenue': Decimal('22500000000'),
                'eps': Decimal('0.37'), 'gross_profit': Decimal('5000000000')
            },
            {
                'company_id': 2, 'ticker': 'RIVN', 'quarter_date': date(2025, 6, 30),
                'quarter_label': '2025-Q2', 'revenue': Decimal('1500000000'),
                'eps': Decimal('-0.50'), 'gross_profit': Decimal('300000000')
            }
        ]
    }


class TestDashboardPerformance:
    """Performance tests for dashboard components."""
    
    def test_large_dataset_handling(self):
        """Test dashboard can handle large datasets efficiently."""
        # Create large sample dataset
        large_df = pd.DataFrame({
            'ticker': ['TSLA'] * 1000 + ['RIVN'] * 1000,
            'quarter_date': [date(2020, 3, 31)] * 2000,
            'revenue': [22.5] * 2000,
            'eps': [0.37] * 2000,
            'gross_profit': [5.0] * 2000
        })
        
        from dashboard import create_revenue_chart
        
        # Should not raise memory errors or take excessive time
        result = create_revenue_chart(large_df)
        assert result is not None
    
    def test_caching_behavior(self):
        """Test that caching decorators work as expected."""
        # This would need to be tested with actual Streamlit environment
        # For now, just verify the decorators are present
        import dashboard
        
        assert hasattr(dashboard.load_financial_data, '__wrapped__')
        assert hasattr(dashboard.load_performance_metrics, '__wrapped__')
        assert hasattr(dashboard.load_comparison_data, '__wrapped__')