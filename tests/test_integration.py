"""
Integration tests for the complete ETL pipeline.
Tests end-to-end workflow with Tesla Q2 2025 validation.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from decimal import Decimal

from main import ETLPipeline
from config import FinancialData
from extract import RateLimitError, APIError
from transform import ValidationError
from load import LoadError


class TestETLPipelineIntegration:
    """Test complete ETL pipeline integration."""
    
    @pytest.fixture
    def pipeline(self):
        """Create ETL pipeline instance for testing."""
        return ETLPipeline()
    
    @pytest.fixture
    def mock_extraction_success(self):
        """Mock successful extraction results with Tesla Q2 2025 data."""
        return {
            'TSLA': {
                'status': 'success',
                'source': 'fmp',
                'income_data': [
                    {
                        "date": "2025-06-30",
                        "symbol": "TSLA",
                        "revenue": 22500000000,  # $22.5B - correct value
                        "eps": 0.40,            # $0.40 - correct value
                        "grossProfit": 5000000000
                    },
                    {
                        "date": "2025-03-31",
                        "symbol": "TSLA",
                        "revenue": 20000000000,
                        "eps": 0.35,
                        "grossProfit": 4500000000
                    }
                ],
                'estimates_data': []
            },
            'RIVN': {
                'status': 'success',  
                'source': 'fmp',
                'income_data': [
                    {
                        "date": "2025-06-30",
                        "symbol": "RIVN",
                        "revenue": 1500000000,
                        "eps": -0.50,
                        "grossProfit": 300000000
                    }
                ],
                'estimates_data': []
            },
            'LCID': {
                'status': 'partial',
                'source': 'yfinance',
                'income_data': [
                    {
                        "date": "2025-06-30",
                        "symbol": "LCID",
                        "revenue": 800000000,
                        "eps": -0.75,
                        "grossProfit": 100000000
                    }
                ],
                'estimates_data': []
            }
        }

    @pytest.fixture
    def mock_extraction_tesla_failure(self):
        """Mock extraction results with Tesla validation failure."""
        return {
            'TSLA': {
                'status': 'success',
                'source': 'fmp',
                'income_data': [
                    {
                        "date": "2025-06-30",
                        "symbol": "TSLA",
                        "revenue": 20000000000,  # Wrong revenue - should be 22.5B
                        "eps": 0.30,            # Wrong EPS - should be 0.40
                        "grossProfit": 4000000000
                    }
                ],
                'estimates_data': []
            }
        }


class TestCompleteETLFlow:
    """Test complete ETL flow scenarios."""
    
    @pytest.fixture
    def pipeline(self):
        return ETLPipeline()
    
    @patch('main.extract_all_companies')
    @patch('load.DatabaseLoader.load_companies')
    @patch('load.DatabaseLoader.load_quarterly_financials')
    @patch('load.DatabaseLoader.validate_tesla_data')
    @patch('load.DatabaseLoader.get_data_summary')
    @patch('transform.DataTransformer.save_to_csv')
    def test_complete_pipeline_success(self, mock_save_csv, mock_summary, mock_validate_db, 
                                     mock_load_financials, mock_load_companies, mock_extract, 
                                     pipeline, mock_extraction_success):
        """Test successful complete pipeline execution."""
        # Mock extraction
        mock_extract.return_value = mock_extraction_success
        
        # Mock transformation (save_to_csv)
        mock_save_csv.return_value = "data/processed/financial_data.csv"
        
        # Mock loading
        mock_load_companies.return_value = {'TSLA': 1, 'RIVN': 2, 'LCID': 3}
        mock_load_financials.return_value = 4  # 4 records loaded
        mock_validate_db.return_value = True   # Tesla validation passes
        mock_summary.return_value = {
            'total_companies': 3,
            'company_breakdown': {
                'TSLA': {'financial_records': 2, 'company_id': 1},
                'RIVN': {'financial_records': 1, 'company_id': 2},
                'LCID': {'financial_records': 1, 'company_id': 3}
            }
        }
        
        # Run pipeline
        result = pipeline.run(['TSLA', 'RIVN', 'LCID'], validate_tesla=True)
        
        # Assertions
        assert result['success'] == True
        assert result['transformation_count'] == 4  # Tesla(2) + RIVN(1) + LCID(1)
        assert result['load_count'] == 4
        assert result['validation_passed'] == True
        assert 'duration' in result
        assert len(result['errors']) == 0
        
        # Verify Tesla Q2 2025 validation was called
        mock_validate_db.assert_called_once()
    
    @patch('main.extract_all_companies')
    def test_pipeline_extraction_failure(self, mock_extract, pipeline):
        """Test pipeline with complete extraction failure."""
        mock_extract.side_effect = APIError("All APIs failed")
        
        with pytest.raises(APIError):
            pipeline.run(['TSLA'], validate_tesla=False)
        
        assert not pipeline.metrics['success']
        assert 'All APIs failed' in pipeline.metrics['errors'][0]
    
    @patch('main.extract_all_companies')
    @patch('transform.DataTransformer.validate_tesla_q2_2025')
    def test_pipeline_tesla_validation_failure(self, mock_tesla_validation, mock_extract, 
                                             pipeline, mock_extraction_tesla_failure):
        """Test pipeline with Tesla Q2 2025 validation failure."""
        mock_extract.return_value = mock_extraction_tesla_failure
        mock_tesla_validation.side_effect = ValidationError("Tesla Q2 2025 revenue validation failed")
        
        with pytest.raises(ValidationError):
            pipeline.run(['TSLA'], validate_tesla=True)
        
        assert not pipeline.metrics['success']
        assert any('Tesla Q2 2025' in error for error in pipeline.metrics['errors'])
    
    @patch('main.extract_all_companies')
    @patch('load.DatabaseLoader.load_quarterly_financials')
    def test_pipeline_loading_failure(self, mock_load_financials, mock_extract, 
                                    pipeline, mock_extraction_success):
        """Test pipeline with database loading failure."""
        mock_extract.return_value = mock_extraction_success
        mock_load_financials.side_effect = LoadError("Database connection failed")
        
        with pytest.raises(LoadError):
            pipeline.run(['TSLA'], validate_tesla=False)
        
        assert not pipeline.metrics['success']
        assert any('Database connection failed' in error for error in pipeline.metrics['errors'])


class TestTeslaValidationIntegration:
    """Test Tesla Q2 2025 validation across the pipeline."""
    
    @pytest.fixture  
    def pipeline(self):
        return ETLPipeline()
    
    @patch('main.extract_all_companies')
    @patch('load.DatabaseLoader.load_companies')
    @patch('load.DatabaseLoader.load_quarterly_financials')
    @patch('load.DatabaseLoader.validate_tesla_data')
    @patch('load.DatabaseLoader.get_data_summary')
    @patch('transform.DataTransformer.save_to_csv')
    def test_tesla_validation_integration_success(self, mock_save_csv, mock_summary,
                                                mock_validate_db, mock_load_financials,
                                                mock_load_companies, mock_extract, 
                                                pipeline, mock_extraction_success):
        """Test Tesla validation passes through entire pipeline."""
        mock_extract.return_value = mock_extraction_success
        mock_save_csv.return_value = "data/processed/financial_data.csv"
        mock_load_companies.return_value = {'TSLA': 1}
        mock_load_financials.return_value = 2
        mock_validate_db.return_value = True
        mock_summary.return_value = {'total_companies': 1, 'company_breakdown': {}}
        
        result = pipeline.run(['TSLA'], validate_tesla=True)
        
        # Both transformation and database validation should pass
        assert result['validation_passed'] == True
        mock_validate_db.assert_called_once()
    
    @patch('main.extract_all_companies')
    def test_tesla_validation_missing_data(self, mock_extract, pipeline):
        """Test pipeline when Tesla Q2 2025 data is completely missing."""
        # Mock Tesla data without Q2 2025
        mock_extract.return_value = {
            'TSLA': {
                'status': 'success',
                'source': 'fmp',
                'income_data': [
                    {
                        "date": "2025-03-31",  # Q1 only, no Q2
                        "symbol": "TSLA",
                        "revenue": 20000000000,
                        "eps": 0.35,
                        "grossProfit": 4500000000
                    }
                ],
                'estimates_data': []
            }
        }
        
        # Should not raise ValidationError, but validation should return False
        with pytest.raises(ValidationError, match="Tesla Q2 2025 data not found"):
            pipeline.run(['TSLA'], validate_tesla=True)


class TestHealthCheckIntegration:
    """Test health check functionality integration."""
    
    @pytest.fixture
    def pipeline(self):
        return ETLPipeline()
    
    @patch('load.DatabaseLoader.get_data_summary')
    def test_health_check_healthy(self, mock_summary, pipeline):
        """Test health check with healthy components."""
        mock_summary.return_value = {
            'total_companies': 3,
            'company_breakdown': {'TSLA': {'financial_records': 2}}
        }
        
        health = pipeline.health_check()
        
        assert health['overall_status'] == 'healthy'
        assert health['components']['database']['status'] == 'healthy'
    
    @patch('load.DatabaseLoader.get_data_summary')
    def test_health_check_unhealthy(self, mock_summary, pipeline):
        """Test health check with unhealthy database."""
        mock_summary.side_effect = Exception("Database connection failed")
        
        health = pipeline.health_check()
        
        assert health['overall_status'] == 'unhealthy'
        assert health['components']['database']['status'] == 'unhealthy'
        assert 'Database connection failed' in health['components']['database']['error']


class TestPipelineEdgeCases:
    """Test edge cases and error scenarios."""
    
    @pytest.fixture
    def pipeline(self):
        return ETLPipeline()
    
    @patch('main.extract_all_companies')
    def test_pipeline_with_empty_results(self, mock_extract, pipeline):
        """Test pipeline behavior with empty extraction results."""
        mock_extract.return_value = {}
        
        with pytest.raises(ValidationError, match="No financial data to transform"):
            pipeline.run(['TSLA'], validate_tesla=False)
    
    @patch('main.extract_all_companies')
    def test_pipeline_partial_success(self, mock_extract, pipeline):
        """Test pipeline with mixed success/failure results."""
        mixed_results = {
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
                ],
                'estimates_data': []
            },
            'RIVN': {
                'status': 'failed',
                'errors': ['API rate limit exceeded', 'yfinance fallback failed']
            }
        }
        
        mock_extract.return_value = mixed_results
        
        with patch('load.DatabaseLoader.load_companies', return_value={'TSLA': 1}), \
             patch('load.DatabaseLoader.load_quarterly_financials', return_value=1), \
             patch('load.DatabaseLoader.validate_tesla_data', return_value=True), \
             patch('load.DatabaseLoader.get_data_summary', return_value={'total_companies': 1, 'company_breakdown': {}}), \
             patch('transform.DataTransformer.save_to_csv', return_value="test.csv"):
            
            result = pipeline.run(['TSLA', 'RIVN'], validate_tesla=True)
            
            # Should succeed with partial data
            assert result['success'] == True
            assert result['transformation_count'] == 1  # Only Tesla data
    
    def test_pipeline_keyboard_interrupt(self, pipeline):
        """Test pipeline handles KeyboardInterrupt gracefully."""
        with patch('main.extract_all_companies', side_effect=KeyboardInterrupt()):
            with pytest.raises(KeyboardInterrupt):
                pipeline.run(['TSLA'], validate_tesla=False)


class TestDataFlowIntegration:
    """Test data flow through all pipeline stages."""
    
    @pytest.fixture
    def pipeline(self):
        return ETLPipeline()
    
    def test_financial_data_objects_flow(self, pipeline):
        """Test FinancialData objects flow correctly through pipeline."""
        # This test verifies that the data structure is maintained correctly
        # through extraction -> transformation -> loading
        
        sample_income_data = [
            {
                "date": "2025-06-30",
                "symbol": "TSLA", 
                "revenue": 22500000000,
                "eps": 0.40,
                "grossProfit": 5000000000
            }
        ]
        
        # Test transformation creates correct FinancialData objects
        financial_data = pipeline.transformer.extract_core_metrics(sample_income_data, "TSLA", "fmp")
        
        assert len(financial_data) == 1
        assert isinstance(financial_data[0], FinancialData)
        assert financial_data[0].ticker == "TSLA"
        assert financial_data[0].quarter_label == "2025-Q2"
        assert financial_data[0].revenue == Decimal('22500000000')
        assert financial_data[0].eps == Decimal('0.40')
        
        # Verify Tesla validation works on these objects
        validation_result = pipeline.transformer.validate_tesla_q2_2025(financial_data)
        assert validation_result == True


if __name__ == "__main__":
    pytest.main([__file__])