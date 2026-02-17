"""
Configuration module for Finance Analytics.
Loads environment variables from .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


class Config:
    """Application configuration loaded from environment variables."""

    # Postgres Configuration
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "ag")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "agpass")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "antigravity")

    # MinIO Configuration
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minio")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minio12345")
    MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "raw")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # Prefect Configuration
    PREFECT_API_URL: str = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

    # Scraping Configuration
    RATE_LIMIT_MIN: int = int(os.getenv("RATE_LIMIT_MIN", "1"))
    RATE_LIMIT_MAX: int = int(os.getenv("RATE_LIMIT_MAX", "5"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Financial Scoring Weights (must sum to ~1.0)
    SCORING_WEIGHTS: dict = {
        "revenue_growth": float(os.getenv("WEIGHT_REVENUE_GROWTH", "0.15")),
        "revenue_qoq": float(os.getenv("WEIGHT_REVENUE_QOQ", "0.10")),
        "net_margin": float(os.getenv("WEIGHT_NET_MARGIN", "0.12")),
        "op_margin": float(os.getenv("WEIGHT_OP_MARGIN", "0.10")),
        "roe": float(os.getenv("WEIGHT_ROE", "0.12")),
        "fcf": float(os.getenv("WEIGHT_FCF", "0.12")),
        "ocf": float(os.getenv("WEIGHT_OCF", "0.08")),
        "debt_to_equity": float(os.getenv("WEIGHT_DEBT_EQUITY", "0.08")),
        "current_ratio": float(os.getenv("WEIGHT_CURRENT_RATIO", "0.05")),
        "eps_growth": float(os.getenv("WEIGHT_EPS_GROWTH", "0.08")),
    }

    @classmethod
    def get_postgres_dsn(cls) -> str:
        """Return PostgreSQL connection string."""
        return (
            f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}"
            f"@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )

    @classmethod
    def get_minio_endpoint_url(cls) -> str:
        """Return MinIO endpoint URL for boto3."""
        return cls.MINIO_ENDPOINT


# Global config instance
config = Config()
