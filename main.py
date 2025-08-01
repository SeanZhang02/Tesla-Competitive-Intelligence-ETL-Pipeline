"""
Main ETL pipeline orchestrator for Tesla Competitive Intelligence.
Coordinates extraction, transformation, and loading of financial data.
"""
import argparse
import logging
import sys
import time
from typing import Dict, List, Any

from config import setup_logging
from extract import extract_all_companies
from transform import DataTransformer, ValidationError
from load import DatabaseLoader, LoadError

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL pipeline orchestrator with error handling and performance tracking."""
    
    def __init__(self):
        self.transformer = DataTransformer()
        self.loader = DatabaseLoader()
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'duration': None,
            'extraction_results': {},
            'transformation_count': 0,
            'load_count': 0,
            'validation_passed': False,
            'errors': []
        }
    
    def run(self, tickers: List[str] = None, validate_tesla: bool = True) -> Dict[str, Any]:
        """Execute the complete ETL pipeline."""
        if tickers is None:
            tickers = ['TSLA', 'RIVN', 'LCID']
        
        self.metrics['start_time'] = time.time()
        logger.info(f"Starting ETL pipeline for {len(tickers)} companies: {', '.join(tickers)}")
        
        try:
            logger.info("Extracting financial data...")
            extraction_results = self._extract_data(tickers)
            self.metrics['extraction_results'] = extraction_results
            
            logger.info("Transforming and standardizing data...")
            financial_data = self._transform_data(extraction_results, validate_tesla)
            self.metrics['transformation_count'] = len(financial_data)
            
            logger.info("Loading data into PostgreSQL...")
            load_count = self._load_data(financial_data)
            self.metrics['load_count'] = load_count
            
            if validate_tesla:
                self.metrics['validation_passed'] = self.loader.validate_tesla_data()
            
            self.metrics['end_time'] = time.time()
            self.metrics['duration'] = self.metrics['end_time'] - self.metrics['start_time']
            self.metrics['success'] = True
            logger.info(f"Pipeline completed successfully in {self.metrics['duration']:.2f}s - {len(financial_data)} records")
            
            return self.metrics
            
        except Exception as e:
            self.metrics['errors'].append(str(e))
            self.metrics['end_time'] = time.time()
            self.metrics['duration'] = self.metrics['end_time'] - self.metrics['start_time']
            self.metrics['success'] = False
            logger.error(f"Pipeline failed after {self.metrics['duration']:.2f}s: {e}")
            raise
    
    def _extract_data(self, tickers: List[str]) -> Dict[str, Dict]:
        """Extract financial data for all companies."""
        try:
            results = extract_all_companies(tickers)
            
            successful = sum(1 for r in results.values() if r['status'] in ['success', 'partial'])
            logger.info(f"Extraction complete: {successful}/{len(tickers)} successful")
            
            for ticker, result in results.items():
                status_icon = "FAILED" if result['status'] == 'failed' else "PARTIAL" if result['status'] == 'partial' else "SUCCESS"
                logger.info(f"{status_icon} {ticker}: {result['status']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Extraction phase failed: {e}")
            raise
    
    def _transform_data(self, extraction_results: Dict[str, Dict], validate_tesla: bool) -> List:
        """Transform and validate extracted data."""
        try:
            financial_data = self.transformer.transform_all_data(extraction_results)
            if not financial_data:
                raise ValidationError("No financial data to transform")
            
            if validate_tesla:
                logger.info("Validating Tesla Q2 2025 data...")
                validation_passed = self.transformer.validate_tesla_q2_2025(financial_data)
                if not validation_passed:
                    logger.warning("Tesla Q2 2025 validation failed - continuing")
            
            csv_path = self.transformer.save_to_csv(financial_data)
            logger.info(f"Saved processed data to {csv_path}")
            return financial_data
            
        except ValidationError as e:
            logger.error(f"Data validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Transformation phase failed: {e}")
            raise
    
    def _load_data(self, financial_data: List) -> int:
        """Load financial data into PostgreSQL."""
        try:
            unique_tickers = list(set([data.ticker for data in financial_data]))
            company_mapping = self.loader.load_companies(unique_tickers)
            logger.info(f"Companies loaded: {list(company_mapping.keys())}")
            
            load_count = self.loader.load_quarterly_financials(financial_data)
            summary = self.loader.get_data_summary()
            total_records = sum(c.get('financial_records', 0) for c in summary.get('company_breakdown', {}).values())
            logger.info(f"Database: {summary.get('total_companies', 0)} companies, {total_records} records")
            
            return load_count
            
        except LoadError as e:
            logger.error(f"Database loading failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Load phase failed: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """Perform basic health check of ETL components."""
        health_status = {'timestamp': time.time(), 'overall_status': 'healthy', 'components': {}}
        
        try:
            summary = self.loader.get_data_summary()
            health_status['components']['database'] = {
                'status': 'healthy' if 'error' not in summary else 'unhealthy',
                'details': summary
            }
        except Exception as e:
            health_status['components']['database'] = {'status': 'unhealthy', 'error': str(e)}
            health_status['overall_status'] = 'unhealthy'
        
        return health_status


def main():
    """Command-line interface for the ETL pipeline."""
    parser = argparse.ArgumentParser(description='Tesla Competitive Intelligence ETL Pipeline')
    parser.add_argument('--tickers', nargs='+', default=['TSLA', 'RIVN', 'LCID'])
    parser.add_argument('--no-validation', action='store_true', help='Skip Tesla Q2 2025 validation')
    parser.add_argument('--health-check', action='store_true', help='Perform health check only')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    pipeline = ETLPipeline()
    
    try:
        if args.health_check:
            health = pipeline.health_check()
            print(f"Health Status: {health['overall_status']}")
            for component, status in health['components'].items():
                print(f"  {component}: {status['status']}")
            return 0 if health['overall_status'] == 'healthy' else 1
        
        validate_tesla = not args.no_validation
        results = pipeline.run(args.tickers, validate_tesla)
        
        if results.get('success', False):
            print(f"Pipeline completed in {results['duration']:.2f}s")
            print(f"Processed {results['transformation_count']}, loaded {results['load_count']}")
            if validate_tesla and results.get('validation_passed'):
                print("Tesla Q2 2025 validation passed")
            return 0
        else:
            print(f"Pipeline failed: {'; '.join(results.get('errors', ['Unknown error']))}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())