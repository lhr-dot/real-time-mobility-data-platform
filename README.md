# 🚌 Real-Time Mobility Data Platform

A real-time Data Engineering platform built with public TBM / Bordeaux Métropole transport data.

## Architecture

```mermaid
flowchart LR
    A["🚌 TBM / Bordeaux Métropole<br/>GTFS-Realtime API"]
    B["🐍 Python Producer"]
    C["🔴 Redpanda / Kafka<br/>vehicle_positions"]
    D["⚡ PySpark<br/>Structured Streaming"]
    E[("🐘 PostgreSQL")]
    F["dbt<br/>Analytics Layer"]
    G["📊 Streamlit<br/>Dashboard"]

    A -->|Vehicle positions| B
    B -->|JSON events| C
    C -->|Streaming events| D
    D -->|Current metrics| E
    D -->|Historical snapshots| E
    E -->|SQL transformations| F
    F --> G
    E -->|Historical data| G
