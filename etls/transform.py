"""
Transform Module — Ticker Data Enrichment & Aggregation
=========================================================
Applies meaningful analytical transformations to raw Coinbase ticker data.

Transformations and **why each matters**
-----------------------------------------

1. **Price Normalisation** (``str → float``):
   Raw WebSocket prices arrive as strings (``"43210.55"``).  We cast to
   ``float`` so every downstream query, dashboard, and ML model can perform
   arithmetic without implicit casting bugs.

2. **Timestamp Conversion** (ISO-8601 → UTC ``datetime``):
   Coinbase timestamps use ISO-8601 with 'Z' or offset suffixes.  We
   normalise to UTC ``datetime`` objects so that time series joins, window
   functions, and timezone-aware dashboards work correctly everywhere.

3. **Spread Calculation** (``best_ask − best_bid``):
   The **bid-ask spread** is the single best proxy for market liquidity.
   A widening spread signals low liquidity or high uncertainty — critical
   for risk dashboards and execution-quality monitoring.

4. **Spread Percentage** (``spread / mid_price × 100``):
   Absolute spread is not comparable across assets at different price levels
   ($50k BTC vs $0.10 DOGE).  Percentage spread normalises this so analysts
   can compare liquidity across all trading pairs on a single chart.

5. **Mid-Price** (``(bid + ask) / 2``):
   The mid-price is a fairer estimate of the "true" market price than the
   last trade price, commonly used in quant finance for mark-to-market.

6. **1-Minute OHLC Aggregation**:
   Building Open-High-Low-Close candles per product per minute turns a
   firehose of tick data into a compact, industry-standard format consumed
   by charting libraries, technical-analysis indicators, and
   volatility-estimation models.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from include.config import get_config, TICKER_TRANSFORMED_SCHEMA
from etls.validate import validate_batch

logger = logging.getLogger("etl.transform")


# ------------------------------------------------------------------ helpers

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Cast to float, returning *default* on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_timestamp(ts_str: str) -> datetime:
    """Parse Coinbase ISO-8601 timestamp into a timezone-aware UTC datetime."""
    # Coinbase may send "2024-06-01T12:00:00.123456Z" or with +00:00
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str).astimezone(timezone.utc)


# ------------------------------------------------------------------ row-level

def transform_ticker_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Apply row-level transformations to a single raw ticker message.

    Returns a new dict — the original is never mutated (functional style
    avoids side-effect bugs).
    """
    price = _safe_float(record.get("price"))
    best_bid = _safe_float(record.get("best_bid"))
    best_ask = _safe_float(record.get("best_ask"))

    spread = best_ask - best_bid
    mid_price = (best_bid + best_ask) / 2.0 if (best_bid + best_ask) else 0.0
    spread_pct = (spread / mid_price * 100.0) if mid_price else 0.0

    ts = _parse_timestamp(record.get("time", datetime.now(timezone.utc).isoformat()))

    return {
        "product_id":     record.get("product_id", "UNKNOWN"),
        "price_usd":      price,
        "timestamp_utc":  ts.isoformat(),
        "best_bid":       best_bid,
        "best_ask":       best_ask,
        "spread":         round(spread, 8),
        "spread_pct":     round(spread_pct, 6),
        "mid_price":      round(mid_price, 8),
        "open_24h":       _safe_float(record.get("open_24h")),
        "high_24h":       _safe_float(record.get("high_24h")),
        "low_24h":        _safe_float(record.get("low_24h")),
        "volume_24h":     _safe_float(record.get("volume_24h")),
        "volume_30d":     _safe_float(record.get("volume_30d")),
        "last_size":      _safe_float(record.get("last_size")),
        "side":           record.get("side", ""),
        "trade_id":       record.get("trade_id", 0),
        "sequence":       record.get("sequence", 0),
        # Partitioning / idempotent metadata
        "ingest_date":    ts.strftime("%Y-%m-%d"),
        "_execution_date": record.get("_execution_date", ""),
    }


# ------------------------------------------------------------------ batch

def transform_ticker_batch(
    raw_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Transform an entire batch of raw ticker records.

    Steps:
    1. Row-level transformations (normalisation, spread, etc.)
    2. Schema validation of the output
    3. Return only valid records.
    """
    logger.info("Transforming %d raw records…", len(raw_records))
    transformed = [transform_ticker_record(r) for r in raw_records]

    valid, n_invalid = validate_batch(
        transformed, TICKER_TRANSFORMED_SCHEMA, drop_invalid=True,
    )
    logger.info(
        "Transformation complete: %d valid, %d dropped", len(valid), n_invalid,
    )
    return valid


# ------------------------------------------------------------------ OHLC

def build_ohlc_1m(records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Aggregate transformed ticker records into 1-minute OHLC candles.

    **Why OHLC?**
    * Reduces data volume by ~60× compared to raw ticks while preserving
      the four most important price reference points per interval.
    * Industry-standard input for technical analysis (Bollinger Bands, RSI,
      MACD) and volatility models (Garman-Klass, Parkinson).
    * Powers candlestick charts in every trading dashboard.

    Returns a ``pd.DataFrame`` with columns:
    ``product_id, timestamp_1m, open, high, low, close, volume, trade_count, vwap, spread_mean``
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    ohlc = (
        df.groupby([
            "product_id",
            pd.Grouper(key="timestamp_utc", freq="1min"),
        ])
        .agg(
            open=("price_usd", "first"),
            high=("price_usd", "max"),
            low=("price_usd", "min"),
            close=("price_usd", "last"),
            volume=("last_size", "sum"),
            trade_count=("price_usd", "count"),
            vwap=("price_usd", "mean"),         # approx VWAP
            spread_mean=("spread", "mean"),
        )
        .reset_index()
        .rename(columns={"timestamp_utc": "timestamp_1m"})
    )

    logger.info("Built %d OHLC-1m candles from %d ticks", len(ohlc), len(df))
    return ohlc
