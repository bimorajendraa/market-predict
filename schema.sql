-- Finance Analytics Database Schema
-- Tables for fetch tracking, news items, and financial facts

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- Table: fetch_jobs
-- Tracks all fetch operations with status and metadata
-- ============================================
CREATE TABLE IF NOT EXISTS fetch_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source VARCHAR(100) NOT NULL,
    ticker VARCHAR(20),
    doc_type VARCHAR(50) NOT NULL,
    url TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    http_code INTEGER,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    checksum VARCHAR(64),
    raw_object_key TEXT,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for querying by source and status
CREATE INDEX IF NOT EXISTS idx_fetch_jobs_source_status ON fetch_jobs(source, status);
CREATE INDEX IF NOT EXISTS idx_fetch_jobs_ticker ON fetch_jobs(ticker);
CREATE INDEX IF NOT EXISTS idx_fetch_jobs_checksum ON fetch_jobs(checksum);

-- ============================================
-- Table: news_items
-- Stores parsed news article metadata
-- ============================================
CREATE TABLE IF NOT EXISTS news_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20),
    source VARCHAR(100) NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    body TEXT,
    checksum VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for querying by ticker and source
CREATE INDEX IF NOT EXISTS idx_news_items_ticker ON news_items(ticker);
CREATE INDEX IF NOT EXISTS idx_news_items_source ON news_items(source);
CREATE INDEX IF NOT EXISTS idx_news_items_published_at ON news_items(published_at);

-- ============================================
-- Table: financial_facts
-- Stores extracted financial metrics from reports
-- ============================================
CREATE TABLE IF NOT EXISTS financial_facts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    metric VARCHAR(100) NOT NULL,
    value DECIMAL(20, 4),
    unit VARCHAR(50),
    currency VARCHAR(10),
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for querying financial data
CREATE INDEX IF NOT EXISTS idx_financial_facts_ticker ON financial_facts(ticker);
CREATE INDEX IF NOT EXISTS idx_financial_facts_period ON financial_facts(period);
CREATE INDEX IF NOT EXISTS idx_financial_facts_metric ON financial_facts(metric);

-- ============================================
-- Table: scores_financial
-- Stores financial scoring results with explainable drivers
-- ============================================
CREATE TABLE IF NOT EXISTS scores_financial (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    score DECIMAL(5, 2) NOT NULL,
    drivers_json JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_financial_ticker ON scores_financial(ticker);
CREATE INDEX IF NOT EXISTS idx_scores_financial_period ON scores_financial(period);

-- ============================================
-- Table: news_sentiment
-- Stores news sentiment analysis with event tagging
-- ============================================
CREATE TABLE IF NOT EXISTS news_sentiment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    headline TEXT,
    sentiment VARCHAR(20) NOT NULL,
    impact DECIMAL(5, 4) DEFAULT 0.0,
    events_json JSONB NOT NULL DEFAULT '[]',
    sources_json JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_sentiment_ticker ON news_sentiment(ticker);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_date ON news_sentiment(date);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_sentiment ON news_sentiment(sentiment);

-- ============================================
-- Table: market_prices
-- Stores daily OHLCV market data
-- ============================================
CREATE TABLE IF NOT EXISTS market_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(20, 4),
    high DECIMAL(20, 4),
    low DECIMAL(20, 4),
    close DECIMAL(20, 4),
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_market_prices_ticker ON market_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_market_prices_date ON market_prices(date);

-- ============================================
-- Table: company_summary
-- Stores generated narrative summaries with evidence
-- ============================================
CREATE TABLE IF NOT EXISTS company_summary (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    rating VARCHAR(20) NOT NULL,
    narrative TEXT NOT NULL,
    evidence_json JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_summary_ticker ON company_summary(ticker);
CREATE INDEX IF NOT EXISTS idx_company_summary_period ON company_summary(period);

-- ============================================
-- Trigger function for updated_at
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables with updated_at column
DROP TRIGGER IF EXISTS update_fetch_jobs_updated_at ON fetch_jobs;
CREATE TRIGGER update_fetch_jobs_updated_at
    BEFORE UPDATE ON fetch_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_news_items_updated_at ON news_items;
CREATE TRIGGER update_news_items_updated_at
    BEFORE UPDATE ON news_items
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Table: filings_raw
-- Stores raw SEC/IDX filings with hash for audit
-- ============================================
CREATE TABLE IF NOT EXISTS filings_raw (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    source VARCHAR(50) NOT NULL,          -- 'sec_edgar', 'idx', 'ir_page'
    filing_type VARCHAR(20) NOT NULL,     -- '10-K', '10-Q', '8-K', 'annual_report'
    filing_date DATE,
    url TEXT NOT NULL,
    sha256 VARCHAR(64),
    stored_path TEXT,                     -- MinIO object key
    accession_number VARCHAR(50),         -- SEC-specific
    cik VARCHAR(20),
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_filings_raw_ticker ON filings_raw(ticker);
CREATE INDEX IF NOT EXISTS idx_filings_raw_type ON filings_raw(filing_type);
CREATE INDEX IF NOT EXISTS idx_filings_raw_date ON filings_raw(filing_date);

-- ============================================
-- Table: filings_extracted
-- Structured metrics extracted from raw filings
-- ============================================
CREATE TABLE IF NOT EXISTS filings_extracted (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filing_id UUID NOT NULL REFERENCES filings_raw(id) ON DELETE CASCADE,
    metric VARCHAR(100) NOT NULL,
    value DECIMAL(20, 4),
    unit VARCHAR(50),
    period_end DATE,
    context VARCHAR(200),                 -- XBRL context or section reference
    confidence DECIMAL(3, 2) DEFAULT 1.0, -- 0-1 extraction confidence
    extractor_version VARCHAR(20) DEFAULT 'v1',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_filings_extracted_filing ON filings_extracted(filing_id);
CREATE INDEX IF NOT EXISTS idx_filings_extracted_metric ON filings_extracted(metric);

-- ============================================
-- Table: thesis
-- Investment thesis per ticker (bull/base/bear)
-- ============================================
CREATE TABLE IF NOT EXISTS thesis (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    sector VARCHAR(50),
    base_thesis TEXT NOT NULL,
    bull_case TEXT,
    bear_case TEXT,
    kpis_json JSONB NOT NULL DEFAULT '[]',
    triggers_json JSONB NOT NULL DEFAULT '[]',
    status VARCHAR(20) DEFAULT 'on_track', -- 'on_track', 'at_risk', 'broken'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_thesis_ticker ON thesis(ticker);

DROP TRIGGER IF EXISTS update_thesis_updated_at ON thesis;
CREATE TRIGGER update_thesis_updated_at
    BEFORE UPDATE ON thesis
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Table: pipeline_runs
-- Audit trail for reproducibility
-- ============================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20),
    run_type VARCHAR(30) NOT NULL DEFAULT 'pipeline', -- 'pipeline', 'memo', 'thesis'
    config_snapshot JSONB NOT NULL DEFAULT '{}',
    sources_json JSONB NOT NULL DEFAULT '[]',
    row_counts_json JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(20) NOT NULL DEFAULT 'running',    -- 'running', 'completed', 'failed'
    error TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_ticker ON pipeline_runs(ticker);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
