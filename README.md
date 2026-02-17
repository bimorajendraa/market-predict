# Finance Analytics

End-to-end Python platform for financial data collection, parsing, scoring, sentiment analysis, and automated summary generation.

> Evolved from **finance-scraper** — original scraping pipeline is fully preserved.

## Features

- 📰 **RSS News Scraping** — Collect news from multiple RSS feeds
- 📄 **Company Reports** — Crawl investor relations pages, download PDF/HTML reports
- 📊 **Financial Parsing** — Extract metrics from HTML/PDF reports with bilingual mapping (EN/ID)
- 🏆 **Financial Scoring** — Score 0-100 with configurable weights (revenue growth, margins, FCF, D/E)
- 🧠 **News Sentiment** — FinBERT (English) + keyword dictionary (Indonesian) + event tagging
- � **Market Prices** — Daily OHLCV from Yahoo Finance
- � **Summary Generator** — Narrative from top financial drivers, news events, and returns
- 🪣 **MinIO Storage** — Raw file storage with structured keys
- 🐘 **PostgreSQL** — 7 tables for full pipeline data
- 🔄 **Prefect Orchestration** — Pipeline: scrape → parse → analyze → summarize

## Tech Stack

- Python 3.11+, Poetry
- Docker Compose (Postgres 16, MinIO, Prefect Server)
- Libraries: requests, beautifulsoup4, lxml, feedparser, boto3, psycopg, prefect, playwright, pdfplumber, transformers (FinBERT), torch, yfinance

## Project Structure

```
finance-analytics/
├── docker-compose.yml          # Docker services
├── schema.sql                  # Database schema (7 tables)
├── .env                        # Environment variables
├── inputs.example.json         # Example input config
├── README.md
└── app/
    ├── pyproject.toml           # Poetry dependencies
    └── src/
        ├── __init__.py
        ├── config.py            # Config + scoring weights
        ├── db.py                # PostgreSQL helpers
        ├── storage.py           # MinIO upload functions
        ├── main.py              # CLI entry point (8 commands)
        ├── collectors/
        │   ├── base.py          # Base collector + retry logic
        │   ├── news_rss.py      # RSS feed scraper
        │   └── company_reports.py
        ├── parsers/
        │   ├── html_parser.py   # HTML report parser
        │   ├── pdf_parser.py    # PDF report parser
        │   └── metric_mapper.py # Account→metric mapping + unit normalization
        ├── analysis/
        │   ├── financial_scoring.py  # Scoring 0-100 + drivers
        │   └── news_sentiment.py     # FinBERT + event tagging
        ├── market/
        │   └── price_fetcher.py      # Yahoo Finance OHLCV
        ├── summary/
        │   └── generator.py          # Narrative summary generator
        └── pipelines/
            └── prefect_flow.py       # Orchestration flow
```

## Quick Start

### Step 1 — Start Docker Services

```bash
docker compose up -d
docker compose ps
```

| Service | URL | Credentials |
|---------|-----|-------------|
| PostgreSQL | localhost:5433 | ag / agpass |
| MinIO API | localhost:9000 | minio / minio12345 |
| MinIO Console | localhost:9001 | minio / minio12345 |
| Prefect UI | localhost:4200 | — |

### Step 2 — Install Python Dependencies

```bash
cd app

# Install Poetry if needed
pip install poetry

# Install all dependencies
poetry install

# Install Playwright browsers (for JS-rendered pages)
poetry run playwright install chromium
```

### Step 3 — Verify Database

```bash
docker exec -it ag-postgres psql -U ag -d antigravity -c "\dt"
```

Expected tables: `fetch_jobs`, `news_items`, `financial_facts`, `scores_financial`, `news_sentiment`, `market_prices`, `company_summary`

> **Note**: If you added tables after initial setup, re-apply the schema:
> ```bash
> docker exec -i ag-postgres psql -U ag -d antigravity < schema.sql
> ```

### Step 4 — Run the Pipeline (Step-by-Step)

From the `app/` directory:

```bash
# ─── STEP A: Scrape ───
# Collect news from RSS feeds
poetry run python -m src.main run-news
poetry run python -m src.main run-news -f "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL"

# Collect company reports (PDF/HTML)
poetry run python -m src.main run-reports
poetry run python -m src.main run-reports --playwright --limit 5

# ─── STEP B: Parse ───
# Parse downloaded reports → financial_facts
poetry run python -m src.main run-parse --ticker AAPL

# ─── STEP C: Fetch Market Data ───
# Download OHLCV from Yahoo Finance
poetry run python -m src.main run-market --ticker AAPL --days 90

# ─── STEP D: Analyze ───
# Run financial scoring + news sentiment
poetry run python -m src.main run-analyze --ticker AAPL --period Q3-2025

# ─── STEP E: Summarize ───
# Generate narrative summary
poetry run python -m src.main run-summary --ticker AAPL --period Q3-2025
```

### Step 5 — Run Full Pipeline (Prefect Flow)

```bash
# Run all steps in sequence: scrape → parse → analyze → summarize
poetry run python -m src.main run-flow --type all
```

## CLI Commands

| Command | Description | Key Options |
|---------|-------------|-------------|
| `run-news` | Scrape RSS feeds | `-f URL`, `-F file.json` |
| `run-reports` | Crawl & download reports | `-p URL`, `--playwright`, `--limit N` |
| `run-parse` | Parse reports → financial_facts | `--ticker TICKER` |
| `run-market` | Fetch OHLCV from Yahoo Finance | `--ticker TICKER`, `--days N` |
| `run-analyze` | Financial scoring + sentiment | `--ticker TICKER`, `--period PERIOD` |
| `run-summary` | Generate narrative summary | `--ticker TICKER`, `--period PERIOD` |
| `run-flow` | Run Prefect orchestration | `--type all\|news\|reports` |
| `check-config` | Display current configuration | — |
| `init-storage` | Initialize MinIO bucket | — |

## Database Tables

### Original Tables

| Table | Purpose |
|-------|---------|
| `fetch_jobs` | Tracks all fetch operations (status, checksum, MinIO key) |
| `news_items` | Parsed news articles (title, body, URL dedup) |
| `financial_facts` | Extracted financial metrics (ticker, period, metric, value, unit, currency) |

### New Analytics Tables

| Table | Purpose |
|-------|---------|
| `scores_financial` | Financial scores 0-100 with `drivers_json` (explainable) |
| `news_sentiment` | Sentiment + impact + `events_json` + `sources_json` |
| `market_prices` | Daily OHLCV data (unique per ticker+date) |
| `company_summary` | Rating + narrative + `evidence_json` (explainable) |

## Financial Scoring

Metrics computed from `financial_facts`:

| Metric | Formula |
|--------|---------|
| `revenue_yoy` | (rev_current − rev_prior_year) / rev_prior_year |
| `revenue_qoq` | (rev_current − rev_prior_quarter) / rev_prior_quarter |
| `net_margin` | net_income / revenue |
| `op_margin` | operating_income / revenue |
| `ocf` | operating_cash_flow (normalized) |
| `fcf` | ocf − capex |
| `debt_to_equity` | total_debt / total_equity |

**Weights** (configurable via env vars):

| Weight | Default | Env Var |
|--------|---------|---------|
| Revenue Growth (YoY) | 0.20 | `WEIGHT_REVENUE_GROWTH` |
| Net Margin | 0.15 | `WEIGHT_NET_MARGIN` |
| Operating Margin | 0.15 | `WEIGHT_OP_MARGIN` |
| Free Cash Flow | 0.15 | `WEIGHT_FCF` |
| Debt/Equity | 0.10 | `WEIGHT_DEBT_EQUITY` |
| Operating Cash Flow | 0.10 | `WEIGHT_OCF` |
| Revenue Growth (QoQ) | 0.15 | `WEIGHT_REVENUE_QOQ` |

## Supported Metric Mapping

The parser maps financial account names (EN + ID) to 10 standard metrics:

| Standard Metric | English Names | Indonesian Names |
|----------------|---------------|-----------------|
| `revenue` | Total Revenue, Net Sales | Pendapatan, Penjualan |
| `gross_profit` | Gross Profit | Laba Kotor |
| `operating_income` | Operating Income | Laba Usaha, Laba Operasi |
| `net_income` | Net Income, Net Profit | Laba Bersih |
| `operating_cash_flow` | Cash from Operations | Arus Kas dari Aktivitas Operasi |
| `capex` | Capital Expenditures | Belanja Modal |
| `total_assets` | Total Assets | Total Aset |
| `total_liabilities` | Total Liabilities | Total Liabilitas |
| `total_equity` | Total Equity | Total Ekuitas |
| `total_debt` | Total Debt | Total Utang |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | localhost | PostgreSQL host |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_USER` | ag | Database user |
| `POSTGRES_PASSWORD` | agpass | Database password |
| `POSTGRES_DB` | antigravity | Database name |
| `MINIO_ENDPOINT` | http://localhost:9000 | MinIO API |
| `MINIO_ACCESS_KEY` | minio | MinIO access key |
| `MINIO_SECRET_KEY` | minio12345 | MinIO secret key |
| `MINIO_BUCKET` | raw | Target bucket |
| `RATE_LIMIT_MIN` | 1 | Min delay (seconds) |
| `RATE_LIMIT_MAX` | 5 | Max delay (seconds) |
| `MAX_RETRIES` | 3 | Retry attempts |
| `LOG_LEVEL` | INFO | Logging level |
| `WEIGHT_*` | (see above) | Scoring weight overrides |

## Verify Results

```bash
# Check all tables
docker exec -it ag-postgres psql -U ag -d antigravity -c \
  "SELECT 'fetch_jobs' as t, count(*) FROM fetch_jobs
   UNION ALL SELECT 'news_items', count(*) FROM news_items
   UNION ALL SELECT 'financial_facts', count(*) FROM financial_facts
   UNION ALL SELECT 'scores_financial', count(*) FROM scores_financial
   UNION ALL SELECT 'news_sentiment', count(*) FROM news_sentiment
   UNION ALL SELECT 'market_prices', count(*) FROM market_prices
   UNION ALL SELECT 'company_summary', count(*) FROM company_summary;"

# View financial scores with drivers
docker exec -it ag-postgres psql -U ag -d antigravity -c \
  "SELECT ticker, period, score, drivers_json FROM scores_financial LIMIT 3;"

# View generated summaries
docker exec -it ag-postgres psql -U ag -d antigravity -c \
  "SELECT ticker, period, rating, LEFT(narrative, 100) FROM company_summary LIMIT 3;"
```

## Cleanup

```bash
docker compose down        # Stop services
docker compose down -v     # Stop + delete all data
```

## Notes

- **Explainability**: All outputs include `drivers_json` or `evidence_json` — no black-box results
- **No fabricated URLs**: All `source_url` fields come from actual scraped data or user input
- **Rate Limiting**: 1-5 second random delay between requests
- **Deduplication**: SHA256 checksum for content, UNIQUE constraint for market prices
- **FinBERT**: First run downloads ~420MB model weights. Subsequent runs use cache
- **Indonesian Support**: Financial keyword dictionary for ID-language reports and sentiment
