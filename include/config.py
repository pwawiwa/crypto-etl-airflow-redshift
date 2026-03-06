"""
Global Configuration for Coinbase WebSocket ETL Pipeline
=========================================================
Centralized configuration module that reads from environment variables
and provides typed, validated settings for all ETL components.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")


# =============================================================================
# Configuration Data Classes
# =============================================================================

@dataclass(frozen=True)
class CoinbaseConfig:
    """Coinbase API and WebSocket settings."""
    #api_key: str = os.getenv("COINBASE_API_KEY", "")
    #api_secret: str = os.getenv("COINBASE_API_SECRET", "")
    ws_url: str = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
    products: list[str] = field(default_factory=lambda: os.getenv(
        "COINBASE_PRODUCTS", "BTC-USD,ETH-USD,SOL-USD,ADA-USD,DOGE-USD,LTC-USD"
    ).split(","))
    channels: list[str] = field(default_factory=lambda: os.getenv(
        "COINBASE_CHANNELS", "ticker"
    ).split(","))
    rate_limit_per_second: int = 8
    connection_timeout_seconds: int = 5


@dataclass(frozen=True)
class S3Config:
    """AWS S3 storage settings."""
    bucket_name: str = os.getenv("S3_BUCKET_NAME", "coinbase-etl-data")
    raw_prefix: str = os.getenv("S3_RAW_PREFIX", "raw/coinbase/ticker")
    transformed_prefix: str = os.getenv("S3_TRANSFORMED_PREFIX", "transformed/coinbase/ticker")
    region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


@dataclass(frozen=True)
class AWSConfig:
    """AWS credentials and region."""
    access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


@dataclass(frozen=True)
class RedshiftConfig:
    """Amazon Redshift data warehouse settings."""
    host: str = os.getenv("REDSHIFT_HOST", "")
    port: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    database: str = os.getenv("REDSHIFT_DB", "coinbase_analytics")
    user: str = os.getenv("REDSHIFT_USER", "admin")
    password: str = os.getenv("REDSHIFT_PASSWORD", "")
    schema: str = os.getenv("REDSHIFT_SCHEMA", "public")
    table: str = os.getenv("REDSHIFT_TABLE", "ticker_transformed")


@dataclass(frozen=True)
class StreamConfig:
    """Streaming ingestion settings."""
    duration_seconds: int = int(os.getenv("STREAM_DURATION_SECONDS", "60"))
    batch_size: int = int(os.getenv("STREAM_BATCH_SIZE", "100"))


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration aggregating all sub-configs."""
    coinbase: CoinbaseConfig = field(default_factory=CoinbaseConfig)
    s3: S3Config = field(default_factory=S3Config)
    aws: AWSConfig = field(default_factory=AWSConfig)
    redshift: RedshiftConfig = field(default_factory=RedshiftConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)


# =============================================================================
# Singleton Config Instance
# =============================================================================
def get_config() -> PipelineConfig:
    """Return the global pipeline configuration (singleton pattern)."""
    return PipelineConfig()


# =============================================================================
# Schema definitions for ticker data validation
# =============================================================================
TICKER_RAW_SCHEMA = {
    "type": "object",
    "required": ["type", "product_id", "price", "time"],
    "properties": {
        "type":           {"type": "string"},
        "product_id":     {"type": "string", "pattern": "^[A-Z]+-[A-Z]+$"},
        "price":          {"type": "string"},
        "time":           {"type": "string"},
        "open_24h":       {"type": "string"},
        "volume_24h":     {"type": "string"},
        "low_24h":        {"type": "string"},
        "high_24h":       {"type": "string"},
        "volume_30d":     {"type": "string"},
        "best_bid":       {"type": "string"},
        "best_bid_size":  {"type": "string"},
        "best_ask":       {"type": "string"},
        "best_ask_size":  {"type": "string"},
        "side":           {"type": "string"},
        "last_size":      {"type": "string"},
        "trade_id":       {"type": "integer"},
        "sequence":       {"type": "integer"},
    },
}

TICKER_TRANSFORMED_SCHEMA = {
    "type": "object",
    "required": [
        "product_id", "price_usd", "timestamp_utc", "best_bid",
        "best_ask", "spread", "spread_pct", "mid_price",
    ],
    "properties": {
        "product_id":     {"type": "string"},
        "price_usd":      {"type": "number"},
        "timestamp_utc":  {"type": "string", "format": "date-time"},
        "best_bid":       {"type": "number"},
        "best_ask":       {"type": "number"},
        "spread":         {"type": "number"},
        "spread_pct":     {"type": "number"},
        "mid_price":      {"type": "number"},
        "open_24h":       {"type": "number"},
        "high_24h":       {"type": "number"},
        "low_24h":        {"type": "number"},
        "volume_24h":     {"type": "number"},
        "volume_30d":     {"type": "number"},
        "last_size":      {"type": "number"},
        "side":           {"type": "string"},
        "trade_id":       {"type": "integer"},
        "sequence":       {"type": "integer"},
        "ingest_date":    {"type": "string", "format": "date"},
    },
}
