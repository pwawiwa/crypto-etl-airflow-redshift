"""
Continuous Streaming Ingestion — Coinbase WebSocket → S3
=========================================================
A long-running process that keeps a persistent WebSocket connection to
Coinbase Advanced Trade API and flushes micro-batches of raw ticker data
to S3 at regular intervals.

**Run this outside Airflow** as a systemd service, Docker container, or
ECS task.  It writes to the *same* S3 raw prefix that the Airflow DAG
reads, so both batch and streaming paths are compatible.

Usage::

    python include/streaming_ingestion.py            # foreground
    nohup python include/streaming_ingestion.py &    # background

Environment variables are loaded from ``.env`` via the global config.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import websocket

# Add project root to path so we can import etls/ and include/
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from include.config import get_config, TICKER_RAW_SCHEMA  # noqa: E402
from etls.validate import validate_record                  # noqa: E402
from etls.load import store_raw_to_s3                      # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streaming_ingestion")


# ------------------------------------------------------------------ Globals
_running = True


def _handle_signal(signum, frame):
    global _running
    logger.info("Received signal %s — shutting down gracefully…", signum)
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ------------------------------------------------------------------ Core

def run_streaming_ingestion() -> None:
    """
    Main streaming loop.

    1. Connect to Coinbase WebSocket.
    2. Subscribe to configured products/channels.
    3. Buffer incoming ticker messages.
    4. Every ``batch_size`` messages, flush buffer to S3.
    5. Repeat until terminated.
    """
    cfg = get_config()
    batch_size = cfg.stream.batch_size
    products = cfg.coinbase.products
    channels = cfg.coinbase.channels
    ws_url = cfg.coinbase.ws_url

    logger.info(
        "Starting streaming ingestion: url=%s products=%s batch_size=%d",
        ws_url, products, batch_size,
    )

    buffer: list[dict[str, Any]] = []
    flush_count = 0

    while _running:
        try:
            ws = websocket.create_connection(
                ws_url,
                timeout=cfg.coinbase.connection_timeout_seconds,
            )
            subscribe_msg = json.dumps({
                "type": "subscribe",
                "product_ids": products,
                "channels": channels,
            })
            ws.send(subscribe_msg)
            logger.info("WebSocket connected & subscribed.")

            while _running:
                try:
                    raw = ws.recv()
                    msg = json.loads(raw)
                except websocket.WebSocketTimeoutException:
                    continue
                except json.JSONDecodeError:
                    logger.warning("Non-JSON message skipped.")
                    continue

                if msg.get("type") != "ticker":
                    continue

                ok, _ = validate_record(msg, TICKER_RAW_SCHEMA)
                if not ok:
                    continue

                msg["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                msg["_execution_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                buffer.append(msg)

                if len(buffer) >= batch_size:
                    _flush(buffer, flush_count)
                    flush_count += 1
                    buffer = []

        except (
            websocket.WebSocketException,
            ConnectionError,
            OSError,
        ) as exc:
            logger.error("WebSocket error: %s — reconnecting in 5s…", exc)
            time.sleep(5)
        except Exception:
            logger.exception("Unexpected error — reconnecting in 10s…")
            time.sleep(10)

    # Flush remaining
    if buffer:
        _flush(buffer, flush_count)
    logger.info("Streaming ingestion stopped. Total flushes: %d", flush_count + 1)


def _flush(buffer: list[dict[str, Any]], batch_num: int) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        key = store_raw_to_s3(buffer, execution_date=today)
        logger.info("Flush #%d: %d records → %s", batch_num, len(buffer), key)
    except Exception:
        logger.exception("Flush #%d FAILED — %d records lost", batch_num, len(buffer))


# ------------------------------------------------------------------ Entry
if __name__ == "__main__":
    run_streaming_ingestion()
