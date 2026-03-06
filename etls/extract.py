"""
Extract Module — Coinbase WebSocket Ticker Stream
===================================================
Connects to the Coinbase Advanced Trade WebSocket API, subscribes to the
``ticker`` channel for the configured products, and collects raw JSON
messages for a configurable window of time.

**Why a time-bounded extract?**
We run the WebSocket in *micro-batch* mode inside an Airflow task so the
DAG remains idempotent — each run captures exactly ``STREAM_DURATION_SECONDS``
of data, tagged with the logical execution date.  A separate long-running
streaming script (``include/streaming_ingestion.py``) is provided for
continuous, always-on ingestion.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import websocket

from include.config import get_config, TICKER_RAW_SCHEMA
from etls.validate import validate_record

logger = logging.getLogger("etl.extract")


# ------------------------------------------------------------------ helpers
def _build_subscribe_message(products: list[str], channels: list[str]) -> str:
    """Build the JSON subscription message required by Coinbase WS."""
    return json.dumps({
        "type": "subscribe",
        "product_ids": products,
        "channels": channels,
    })


# ------------------------------------------------------------------ public API
def extract_coinbase_stream(
    execution_date: str | None = None,
    duration_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """
    Connect to Coinbase WS, subscribe to ticker, and return raw messages.

    Parameters
    ----------
    execution_date : str, optional
        Airflow logical date in ISO format — stamped onto every record for
        idempotent partitioning.
    duration_seconds : int, optional
        Override the default stream window from config.

    Returns
    -------
    list[dict]
        Raw ticker messages enriched with ``_ingested_at`` and
        ``_execution_date`` metadata fields.

    Raises
    ------
    ConnectionError
        When the WebSocket fails to connect within the timeout window.
    """
    cfg = get_config()
    duration = duration_seconds or cfg.stream.duration_seconds
    products = cfg.coinbase.products
    channels = cfg.coinbase.channels
    ws_url = cfg.coinbase.ws_url

    logger.info(
        "Extracting from %s | products=%s channels=%s duration=%ss",
        ws_url, products, channels, duration,
    )

    records: list[dict[str, Any]] = []
    start_ts = time.time()

    ws = websocket.create_connection(
        ws_url,
        timeout=cfg.coinbase.connection_timeout_seconds,
    )
    try:
        # Must subscribe within 5 seconds per Coinbase docs
        ws.send(_build_subscribe_message(products, channels))
        logger.info("Subscribe message sent, waiting for confirmation…")

        while time.time() - start_ts < duration:
            try:
                raw = ws.recv()
                msg = json.loads(raw)
            except (websocket.WebSocketTimeoutException, json.JSONDecodeError) as exc:
                logger.warning("Recv error: %s — skipping", exc)
                continue

            # Detect subscription errors from Coinbase
            if msg.get("type") == "error":
                error_msg = msg.get("message", "Unknown error")
                raise ConnectionError(
                    f"Coinbase WebSocket error: {error_msg}. "
                    "Check API credentials and subscription parameters."
                )

            # Log subscription confirmation
            if msg.get("type") == "subscriptions":
                logger.info("Subscription confirmed: %s", msg.get("channels", []))
                continue

            # Skip non-ticker control messages (heartbeats, etc.)
            if msg.get("type") != "ticker":
                logger.debug("Skipping non-ticker message type=%s", msg.get("type"))
                continue

            # Validate against raw schema
            is_valid, errors = validate_record(msg, TICKER_RAW_SCHEMA)
            if not is_valid:
                logger.warning("Schema validation failed: %s — record: %s", errors, msg)
                continue

            # Enrich with metadata
            msg["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            msg["_execution_date"] = execution_date or datetime.now(timezone.utc).date().isoformat()

            records.append(msg)
            if len(records) % 50 == 0:
                logger.info("Collected %d records so far…", len(records))

    finally:
        ws.close()
        logger.info(
            "WebSocket closed. Total records collected: %d in %.1fs",
            len(records), time.time() - start_ts,
        )

    return records
