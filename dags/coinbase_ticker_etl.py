"""
Coinbase Ticker ETL DAG — Airflow 3.x
=======================================
An idempotent, retry-enabled pipeline that:

1. **Extracts** real-time ticker data from Coinbase Advanced Trade WebSocket
2. **Stores raw** JSON on S3 (partitioned by date)
3. **Transforms** — price normalisation, timestamp conversion, spread
   calculation, 1-min OHLC aggregation
4. **Loads** into a Redshift data warehouse

Schedule: Every 5 minutes — each run captures ``STREAM_DURATION_SECONDS``
of live market data.

Idempotency:
    Re-running the same ``logical_date`` overwrites the same S3 partition
    and DELETEs-then-COPYs the same Redshift rows.

Asset-based scheduling (Airflow 3.x):
    Downstream DAGs/dashboards can subscribe to the ``coinbase_ticker_transformed``
    asset to react when new data lands.
"""

from __future__ import annotations

import logging

from airflow.decorators import dag, task
from airflow.sdk import Asset, get_current_context
from pendulum import datetime, duration, now

logger = logging.getLogger("dag.coinbase_etl")


def _get_logical_date_str() -> str:
    """Get logical_date as YYYY-MM-DD from Airflow context.

    In Airflow 3.x, manual triggers may not populate `logical_date`
    in the context dict. Fall back to today (UTC) in that case.
    """
    ctx = get_current_context()
    ld = ctx.get("logical_date")
    if ld is not None:
        return ld.to_date_string() if hasattr(ld, "to_date_string") else str(ld)[:10]
    # Fallback: use dag_run.logical_date or current UTC date
    dag_run = ctx.get("dag_run")
    if dag_run and getattr(dag_run, "logical_date", None) is not None:
        return str(dag_run.logical_date)[:10]
    return now("UTC").to_date_string()

# ---- Airflow 3.x Assets (formerly Datasets) --------------------------------
RAW_S3_ASSET = Asset("coinbase_ticker_raw_s3")
TRANSFORMED_S3_ASSET = Asset("coinbase_ticker_transformed_s3")
OHLC_S3_ASSET = Asset("coinbase_ohlc_1m_s3")
WAREHOUSE_ASSET = Asset("coinbase_ticker_warehouse")


@dag(
    dag_id="coinbase_ticker_etl",
    start_date=datetime(2026, 3, 6),
    schedule=duration(minutes=5),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["coinbase", "etl", "crypto", "aws"],
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=5),
    },
)
def coinbase_ticker_etl():
    """Main Coinbase Ticker ETL pipeline."""

    # ---- Task 1: Extract live ticker stream ---------------------------------
    @task(
        task_id="extract_coinbase_stream",
        outlets=[RAW_S3_ASSET],
    )
    def extract_coinbase_stream() -> list[dict]:
        """
        Connect to Coinbase WebSocket, subscribe to the ticker channel,
        and collect raw messages for ``STREAM_DURATION_SECONDS``.
        """
        from etls.extract import extract_coinbase_stream as do_extract

        logical_date = _get_logical_date_str()
        logger.info("Starting extraction for logical_date=%s", logical_date)

        records = do_extract(execution_date=logical_date)
        if not records:
            raise RuntimeError(
                f"Extracted 0 records for {logical_date}. "
                "Check WebSocket connectivity and Coinbase API authentication."
            )
        logger.info("Extracted %d ticker records", len(records))
        return records

    # ---- Task 2: Persist raw JSON on S3 -------------------------------------
    @task(
        task_id="store_raw_s3",
        outlets=[RAW_S3_ASSET],
    )
    def store_raw_s3(records: list[dict]) -> str:
        """
        Write raw ticker data as newline-delimited JSON to S3,
        partitioned by execution date for idempotent reprocessing.
        """
        from etls.load import store_raw_to_s3

        if not records:
            raise ValueError("store_raw_s3 received empty records list.")

        logical_date = _get_logical_date_str()
        s3_key = store_raw_to_s3(records, execution_date=logical_date)
        logger.info("Raw data stored → %s", s3_key)
        return s3_key

    # ---- Task 3: Transform ticker data --------------------------------------
    @task(
        task_id="transform_ticker_data",
        outlets=[TRANSFORMED_S3_ASSET, OHLC_S3_ASSET],
    )
    def transform_ticker_data(records: list[dict]) -> dict:
        """
        Apply transformations:
        - Price normalisation (str → float)
        - Timestamp conversion to UTC
        - Bid-ask spread & spread % calculation
        - Mid-price derivation
        - 1-minute OHLC candle aggregation
        """
        from etls.transform import transform_ticker_batch, build_ohlc_1m
        from etls.load import store_transformed_to_s3, store_ohlc_to_s3

        if not records:
            raise ValueError("transform_ticker_data received empty records list.")

        logical_date = _get_logical_date_str()

        # Row-level transforms
        transformed = transform_ticker_batch(records)
        if not transformed:
            raise RuntimeError(
                "All records were dropped during transformation/validation. "
                "Check schema and raw data quality."
            )
        logger.info("Transformed %d records", len(transformed))

        # Store transformed Parquet to S3
        parquet_keys = store_transformed_to_s3(transformed, execution_date=logical_date)

        # Build & store OHLC
        ohlc_df = build_ohlc_1m(transformed)
        ohlc_key = store_ohlc_to_s3(ohlc_df, execution_date=logical_date)

        return {
            "transformed_count": len(transformed),
            "parquet_keys": parquet_keys,
            "ohlc_key": ohlc_key,
            "ohlc_candle_count": len(ohlc_df),
        }

    # ---- Task 4: Load into Redshift warehouse --------------------------------
    @task(
        task_id="load_to_warehouse",
        outlets=[WAREHOUSE_ASSET],
    )
    def load_to_warehouse(transform_result: dict) -> int:
        """
        COPY transformed Parquet from S3 into Redshift.
        Idempotent: deletes existing partition, then loads.
        """
        from etls.load import load_to_redshift

        logical_date = _get_logical_date_str()
        s3_keys = transform_result.get("parquet_keys", [])
        if not s3_keys:
            raise ValueError("No parquet keys to load — upstream transform produced no output.")

        rows = load_to_redshift(s3_keys, execution_date=logical_date)
        logger.info("Loaded %d rows into Redshift", rows)
        return rows

    # ---- Wiring: linear dependency chain ------------------------------------
    raw = extract_coinbase_stream()
    raw_key = store_raw_s3(raw)       # noqa: F841 — used for dependency
    result = transform_ticker_data(raw)
    load_to_warehouse(result)


# Instantiate the DAG
coinbase_ticker_etl()
