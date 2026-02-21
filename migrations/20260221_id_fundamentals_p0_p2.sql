-- Incremental migration: Indonesia fundamentals + watchlist/backtest support
-- Date: 2026-02-21

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS idx_filings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'idx_ir',
    filing_type VARCHAR(50) NOT NULL,
    filing_date DATE,
    period VARCHAR(20),
    url TEXT NOT NULL,
    checksum VARCHAR(64),
    doc_kind VARCHAR(50),
    title TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (ticker, url)
);

CREATE INDEX IF NOT EXISTS idx_idx_filings_ticker ON idx_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_idx_filings_period ON idx_filings(period);
CREATE INDEX IF NOT EXISTS idx_idx_filings_date ON idx_filings(filing_date);

CREATE TABLE IF NOT EXISTS fundamentals_quarterly (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    statement_date DATE,
    currency VARCHAR(10) NOT NULL DEFAULT 'IDR',
    unit VARCHAR(20) NOT NULL DEFAULT 'raw',
    scale VARCHAR(10) NOT NULL DEFAULT '1',
    revenue DECIMAL(30, 4),
    operating_income DECIMAL(30, 4),
    net_income DECIMAL(30, 4),
    eps DECIMAL(20, 6),
    total_assets DECIMAL(30, 4),
    total_equity DECIMAL(30, 4),
    total_debt DECIMAL(30, 4),
    shares_outstanding DECIMAL(30, 4),
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (ticker, period)
);

CREATE INDEX IF NOT EXISTS idx_fq_ticker ON fundamentals_quarterly(ticker);
CREATE INDEX IF NOT EXISTS idx_fq_period ON fundamentals_quarterly(period);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_fundamentals_quarterly_updated_at ON fundamentals_quarterly;
CREATE TRIGGER update_fundamentals_quarterly_updated_at
    BEFORE UPDATE ON fundamentals_quarterly
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS bank_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    period VARCHAR(20) NOT NULL,
    statement_date DATE,
    currency VARCHAR(10) NOT NULL DEFAULT 'IDR',
    unit VARCHAR(20) NOT NULL DEFAULT 'ratio',
    scale VARCHAR(10) NOT NULL DEFAULT '1',
    nim DECIMAL(10, 6),
    npl DECIMAL(10, 6),
    car_kpmm DECIMAL(10, 6),
    ldr DECIMAL(10, 6),
    casa DECIMAL(10, 6),
    bopo DECIMAL(10, 6),
    cost_of_credit DECIMAL(10, 6),
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (ticker, period)
);

CREATE INDEX IF NOT EXISTS idx_bank_metrics_ticker ON bank_metrics(ticker);
CREATE INDEX IF NOT EXISTS idx_bank_metrics_period ON bank_metrics(period);

DROP TRIGGER IF EXISTS update_bank_metrics_updated_at ON bank_metrics;
CREATE TRIGGER update_bank_metrics_updated_at
    BEFORE UPDATE ON bank_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS corporate_actions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(20) NOT NULL,
    action_date DATE NOT NULL,
    action_type VARCHAR(30) NOT NULL,
    amount DECIMAL(30, 8),
    currency VARCHAR(10),
    ratio VARCHAR(30),
    shares_outstanding DECIMAL(30, 4),
    payout_date DATE,
    source_url TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE NULLS NOT DISTINCT (ticker, action_date, action_type, ratio)
);

CREATE INDEX IF NOT EXISTS idx_corp_actions_ticker ON corporate_actions(ticker);
CREATE INDEX IF NOT EXISTS idx_corp_actions_date ON corporate_actions(action_date);
CREATE INDEX IF NOT EXISTS idx_corp_actions_type ON corporate_actions(action_type);
