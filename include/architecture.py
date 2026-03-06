"""
Architecture Diagram Generator
================================
Generates a Mermaid architecture diagram for the Coinbase ETL pipeline.
Also provides ASCII fallback for terminals.
"""


def get_mermaid_diagram() -> str:
    """Return the Mermaid-syntax architecture diagram."""
    return """
```mermaid
flowchart LR
    subgraph Sources
        CB["🔌 Coinbase\\nWebSocket API"]
    end

    subgraph Ingestion
        SI["🔄 Streaming\\nIngestion\\n(always-on)"]
        AF_E["✈️ Airflow Task\\nextract_coinbase_stream"]
    end

    subgraph "AWS S3 Data Lake"
        S3_RAW["📦 S3 Raw\\nraw/coinbase/ticker/\\ndate=YYYY-MM-DD/\\n(JSONL)"]
        S3_TRANS["📦 S3 Transformed\\ntransformed/coinbase/ticker/\\ndate=YYYY-MM-DD/\\nproduct=XXX/\\n(Parquet)"]
        S3_OHLC["📦 S3 OHLC\\nohlc_1m/date=YYYY-MM-DD/\\n(Parquet)"]
    end

    subgraph "Airflow 3.x DAG"
        T1["1️⃣ extract_coinbase_stream"]
        T2["2️⃣ store_raw_s3"]
        T3["3️⃣ transform_ticker_data\\n• price normalisation\\n• timestamp → UTC\\n• spread calc\\n• 1-min OHLC"]
        T4["4️⃣ load_to_warehouse"]
    end

    subgraph Warehouse
        RS["🏢 Amazon Redshift\\nticker_transformed"]
    end

    subgraph Analytics
        STR["📊 Streamlit\\nReal-Time Dashboard"]
        ATH["🔍 Athena / Spectrum\\nAd-hoc queries"]
    end

    CB -->|WSS tick stream| SI
    CB -->|WSS tick stream| AF_E
    SI -->|micro-batch flush| S3_RAW
    AF_E --> T1
    T1 --> T2
    T1 --> T3
    T2 --> S3_RAW
    T3 --> S3_TRANS
    T3 --> S3_OHLC
    T3 --> T4
    T4 -->|COPY| RS
    S3_TRANS --> ATH
    S3_OHLC --> STR
    RS --> STR
    CB -->|live WSS| STR
```
"""


def get_ascii_diagram() -> str:
    """Return an ASCII architecture overview."""
    return """
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COINBASE WEBSOCKET ETL — ARCHITECTURE                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐                                                         │
│   │  Coinbase     │    WSS (ticker, BTC/ETH/SOL)                           │
│   │  WebSocket    │─────────────┬──────────────────────┐                   │
│   │  API          │             │                      │                   │
│   └──────────────┘             │                      │                   │
│                                 ▼                      ▼                   │
│              ┌──────────────────────┐   ┌──────────────────────┐           │
│              │ Streaming Ingestion  │   │ Airflow 3.x DAG      │           │
│              │ (always-on process)  │   │ (every 5 min)        │           │
│              └─────────┬────────────┘   │                      │           │
│                        │                │  1. extract_stream   │           │
│                        │                │  2. store_raw_s3     │           │
│                        │                │  3. transform_data   │           │
│                        │                │  4. load_warehouse   │           │
│                        │                └──────┬───────────────┘           │
│                        │                       │                           │
│                        ▼                       ▼                           │
│   ┌────────────────────────────────────────────────────────────┐           │
│   │                     AWS S3 Data Lake                       │           │
│   │  ┌────────────────┐ ┌──────────────────┐ ┌──────────────┐ │           │
│   │  │ raw/           │ │ transformed/     │ │ ohlc_1m/     │ │           │
│   │  │  date=YYYY-...│ │  date=YYYY-.../  │ │  date=YYYY...│ │           │
│   │  │  (JSONL)      │ │  product=XXX/   │ │  (Parquet)   │ │           │
│   │  │               │ │  (Parquet)      │ │              │ │           │
│   │  └────────────────┘ └────────┬─────────┘ └──────┬───────┘ │           │
│   └──────────────────────────────┼──────────────────┼──────────┘           │
│                                  │                  │                      │
│                                  ▼                  │                      │
│                    ┌──────────────────────┐          │                      │
│                    │  Amazon Redshift     │          │                      │
│                    │  (Data Warehouse)    │          │                      │
│                    └──────────┬───────────┘          │                      │
│                               │                     │                      │
│                               ▼                     ▼                      │
│                    ┌──────────────────────────────────────┐                 │
│                    │       Streamlit Dashboard            │                 │
│                    │  • Live price ticker                 │                 │
│                    │  • OHLC candlestick charts           │                 │
│                    │  • Bid-ask spread time-series        │                 │
│                    │  • Volume spike detection            │                 │
│                    │  • Rolling volatility gauge          │                 │
│                    └──────────────────────────────────────┘                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
"""


if __name__ == "__main__":
    print(get_ascii_diagram())
    print("\n\nMermaid source (paste into mermaid.live):")
    print(get_mermaid_diagram())
