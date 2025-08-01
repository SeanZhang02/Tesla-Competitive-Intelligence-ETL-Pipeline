# Tesla Competitive Intelligence ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791.svg)](https://postgresql.org)
[![Tests](https://img.shields.io/badge/Tests-95%25%20Coverage-green.svg)](./tests)
[![Code Style](https://img.shields.io/badge/Code%20Style-Black-black.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Production-ready ETL pipeline that extracts, transforms, and loads quarterly financial data for Tesla and its EV competitors (Rivian, Lucid Motors) into PostgreSQL for competitive analysis.

## 🎯 Business Context

This pipeline enables financial analysts to:
- **Track Tesla's performance** against key EV competitors (RIVN, LCID)
- **Monitor quarterly trends** in Revenue, EPS, and Gross Profit
- **Validate data quality** with Tesla Q2 2025 benchmarks ($22.5B revenue, $0.3709 EPS)
- **Export standardized datasets** for further analysis and reporting

**Portfolio Note**: This project demonstrates professional ETL development skills, clean Python architecture, financial data handling, and comprehensive testing practices.

## 🏗️ Architecture

### Modular ETL Design
```
├── extract.py      # Data extraction from Financial Modeling Prep + yfinance fallback
├── transform.py    # Data standardization, validation, and Tesla Q2 2025 checks  
├── load.py         # PostgreSQL bulk loading with transaction management
├── main.py         # Pipeline orchestration and CLI interface
└── config.py       # Settings, database models, and Pydantic validation
```

### Data Flow
```
Financial APIs → Raw JSON Files → Standardized FinancialData Objects → PostgreSQL
     ↓              ↓                        ↓                           ↓
FMP/yfinance → data/raw/*.json → Decimal precision → Normalized schema
```

### Database Schema
```sql
companies
├── id (PRIMARY KEY)
├── ticker (UNIQUE)          # TSLA, RIVN, LCID
├── name                     # Company full name
└── sector                   # Electric Vehicles

quarterly_financials  
├── id (PRIMARY KEY)
├── company_id (FK)          # Links to companies.id
├── quarter_date             # End date of quarter
├── quarter_label            # Standardized "YYYY-QN" format
├── revenue (DECIMAL)        # Quarterly revenue in dollars
├── eps (DECIMAL)            # Earnings per share  
├── gross_profit (DECIMAL)   # Gross profit in dollars
└── updated_at               # Data freshness tracking

analyst_estimates
├── id (PRIMARY KEY)  
├── company_id (FK)
├── quarter_date
├── estimated_revenue
├── estimated_eps
└── analyst_count
```

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- Financial Modeling Prep API key (free tier: 250 calls/day)

### 1. Environment Setup
```bash
# Clone repository
git clone <repository-url>
cd tesla-competitive-etl-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
FMP_API_KEY=your_financial_modeling_prep_api_key
DATABASE_URL=postgresql://username:password@localhost:5432/tesla_etl
```

### 3. Database Setup
```bash
# Create PostgreSQL database
createdb tesla_etl

# Run schema creation
psql -d tesla_etl -f schema.sql
```

### 4. Run Pipeline
```bash
# Full pipeline with Tesla validation
python main.py

# Custom tickers
python main.py --tickers TSLA RIVN LCID

# Skip Tesla validation
python main.py --no-validation

# Health check
python main.py --health-check
```

## 📊 Data Sources & API Integration

### Primary: Financial Modeling Prep
- **Endpoint**: `/income-statement` (quarterly)
- **Rate Limit**: 250 calls/day (free tier)
- **Data Quality**: High accuracy, comprehensive coverage
- **Fallback**: Automatic failover to yfinance on rate limits

### Secondary: yfinance (Yahoo Finance)
- **Usage**: Fallback when FMP quota exceeded
- **Rate Limit**: None (reasonable use)
- **Data Quality**: Good for major tickers, some gaps for newer companies

## 🔍 Data Validation & Quality

### Tesla Q2 2025 Validation
The pipeline validates against known Tesla Q2 2025 results:
- **Revenue**: $22.5 billion (exact match required)
- **EPS**: $0.3709 (tolerance: ±$0.01)

### Data Quality Checks
- **Quarter Standardization**: All dates → "YYYY-QN" format
- **Decimal Precision**: Financial values use `Decimal` type (not float)
- **Missing Data Handling**: Graceful handling of null/missing values
- **Duplicate Prevention**: Upsert logic prevents duplicate records

## 🧪 Testing

### Run All Tests
```bash
# Unit tests
pytest tests/test_extract.py tests/test_transform.py tests/test_load.py

# Integration tests  
pytest tests/test_integration.py

# Edge cases
pytest tests/test_edge_cases.py

# Full test suite
pytest tests/ -v
```

### Test Coverage
- **Unit Tests**: 95%+ coverage for core modules
- **Integration Tests**: End-to-end pipeline scenarios
- **Edge Cases**: API failures, missing data, network issues
- **Tesla Validation**: Automated Q2 2025 data verification

## 📈 Usage Examples

### Basic Pipeline Run
```bash
python main.py
# Output:
# 🚀 Starting ETL pipeline for 3 companies: TSLA, RIVN, LCID
# 📥 Extracting financial data...
# 🔄 Transforming and standardizing data...
# 📤 Loading data into PostgreSQL...
# ✅ Tesla Q2 2025 validation passed
# 🎉 Pipeline completed in 45.23s - 12 records
```

### Health Check
```bash
python main.py --health-check
# Output:
# Health Status: healthy
#   database: healthy
```

### Custom Ticker Analysis
```bash
python main.py --tickers TSLA F GM --no-validation
# Analyze Tesla vs traditional automakers
```

## 📁 Output Files

### Generated Data
```
data/
├── raw/                    # Raw API responses (JSON)
│   ├── TSLA_income_raw.json
│   ├── RIVN_income_raw.json
│   └── LCID_income_raw.json
├── processed/              # Cleaned datasets (CSV) 
│   └── financial_data_YYYY-MM-DD.csv
└── logs/                   # Application logs
    └── etl_pipeline.log
```

### CSV Export Format
| ticker | quarter_date | quarter_label | revenue      | eps  | gross_profit |
|--------|-------------|---------------|--------------|------|-------------|
| TSLA   | 2025-06-30  | 2025-Q2      | 22500000000  | 0.40 | 5000000000  |
| RIVN   | 2025-06-30  | 2025-Q2      | 1500000000   | -0.50| 300000000   |

## ⚙️ Configuration Options

### Environment Variables
```bash
# API Configuration
FMP_API_KEY=your_api_key_here          # Required
FMP_RATE_LIMIT=250                     # Calls per day

# Database Configuration  
DATABASE_URL=postgresql://...           # Required
DB_POOL_SIZE=20                        # Connection pool size
DB_POOL_RECYCLE=3600                   # Connection recycle time

# Logging Configuration
LOG_LEVEL=INFO                         # DEBUG, INFO, WARNING, ERROR
LOG_FILE_MAX_BYTES=10485760           # 10MB default
LOG_BACKUP_COUNT=5                     # Number of backup files
```

### Command Line Options
```bash
python main.py --help

options:
  --tickers TSLA RIVN LCID    # Specify tickers to process
  --no-validation             # Skip Tesla Q2 2025 validation  
  --health-check              # Perform system health check
  --verbose, -v               # Enable debug logging
```

## 🔧 Development

### Code Standards
- **Line Limits**: extract.py, transform.py, load.py ≤ 300 lines; main.py ≤ 200 lines; config.py ≤ 150 lines
- **Type Hints**: All functions include type annotations
- **Docstrings**: Google-style docstrings for all public functions
- **Error Handling**: Comprehensive exception handling with logging

### Adding New Companies
```python
# 1. Add to config.py company names mapping
company_names = {
    'TSLA': 'Tesla Inc',
    'RIVN': 'Rivian Automotive Inc', 
    'LCID': 'Lucid Group Inc',
    'F': 'Ford Motor Company'  # New addition
}

# 2. Run pipeline with new ticker
python main.py --tickers TSLA RIVN LCID F
```

## 🚨 Error Handling & Monitoring

### Automatic Fallbacks
- **API Rate Limits**: FMP → yfinance automatic failover
- **Network Issues**: 3 retry attempts with exponential backoff
- **Data Missing**: Graceful handling with null value insertion
- **Database Errors**: Transaction rollback and error logging

### Monitoring
- **Health Checks**: `/health-check` endpoint for system monitoring
- **Structured Logging**: JSON-formatted logs for log aggregation
- **Performance Metrics**: Execution time and record count tracking
- **Data Quality Alerts**: Failed Tesla validation triggers warnings

## 🔒 Security & Best Practices

### API Key Management
- Environment variables (never hardcoded)
- `.env` file excluded from version control
- API key rotation support

### Database Security
- Connection pooling with automatic cleanup
- SQL injection prevention via SQLAlchemy ORM
- Transaction isolation for data consistency

### Data Privacy
- No PII collection (only public financial data)
- Raw data files stored locally only
- Configurable data retention policies

## 📋 Troubleshooting

### Common Issues

**API Rate Limit Exceeded**
```bash
ERROR: Daily API limit (250) would be exceeded
Solution: Wait 24 hours or upgrade FMP plan
```

**Database Connection Failed**
```bash
ERROR: Database loading failed: connection to server failed
Solution: Check DATABASE_URL and PostgreSQL service status
```

**Tesla Validation Failed**
```bash
ERROR: Tesla Q2 2025 revenue validation failed
Solution: Check data source accuracy or run with --no-validation
```

### Support
- Check logs in `data/logs/etl_pipeline.log`
- Run health check: `python main.py --health-check`
- Verify configuration: `python -c "from config import Settings; Settings()"`

## 📊 Performance Metrics

### Typical Execution Times
- **3 Companies (TSLA, RIVN, LCID)**: 30-60 seconds
- **Single Company**: 10-20 seconds  
- **Database Health Check**: <5 seconds

### Resource Usage
- **Memory**: ~50MB peak (10K records)
- **Disk**: ~10MB per quarterly dataset
- **Network**: ~2MB API data transfer

---

**Portfolio Project**: This ETL pipeline demonstrates professional software development practices including modular architecture, comprehensive testing, data validation, error handling, and production-ready deployment considerations.