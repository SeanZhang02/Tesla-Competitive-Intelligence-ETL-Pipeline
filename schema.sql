-- Tesla Competitive Intelligence ETL Pipeline Database Schema
-- PostgreSQL schema for storing Tesla and EV competitors financial data

-- Create database (run this separately)
-- CREATE DATABASE competitor_intelligence;

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    sector VARCHAR(50) DEFAULT 'Electric Vehicles',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quarterly financials table
CREATE TABLE IF NOT EXISTS quarterly_financials (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    quarter_date DATE NOT NULL,
    quarter_label VARCHAR(10) NOT NULL, -- Format: "2025-Q2"
    revenue DECIMAL(15, 2),
    eps DECIMAL(10, 4), -- Earnings per share with 4 decimal precision
    gross_profit DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique combination of company and quarter
    CONSTRAINT unique_company_quarter UNIQUE (company_id, quarter_date)
);

-- Analyst estimates table
CREATE TABLE IF NOT EXISTS analyst_estimates (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    quarter_date DATE NOT NULL,
    quarter_label VARCHAR(10) NOT NULL, -- Format: "2025-Q2"
    estimated_revenue DECIMAL(15, 2),
    estimated_eps DECIMAL(10, 4),
    analyst_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique combination of company and quarter
    CONSTRAINT unique_company_quarter_estimates UNIQUE (company_id, quarter_date)
);

-- Indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_company_quarter ON quarterly_financials(company_id, quarter_date);
CREATE INDEX IF NOT EXISTS idx_quarter_label ON quarterly_financials(quarter_label);
CREATE INDEX IF NOT EXISTS idx_company_ticker ON companies(ticker);
CREATE INDEX IF NOT EXISTS idx_estimates_company_quarter ON analyst_estimates(company_id, quarter_date);
CREATE INDEX IF NOT EXISTS idx_estimates_quarter_label ON analyst_estimates(quarter_label);

-- Insert default companies
INSERT INTO companies (ticker, name, sector) VALUES 
    ('TSLA', 'Tesla Inc', 'Electric Vehicles'),
    ('RIVN', 'Rivian Automotive Inc', 'Electric Vehicles'),
    ('LCID', 'Lucid Group Inc', 'Electric Vehicles')
ON CONFLICT (ticker) DO NOTHING;

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update triggers
CREATE TRIGGER update_companies_modtime 
    BEFORE UPDATE ON companies 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_quarterly_financials_modtime 
    BEFORE UPDATE ON quarterly_financials 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_analyst_estimates_modtime 
    BEFORE UPDATE ON analyst_estimates 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();