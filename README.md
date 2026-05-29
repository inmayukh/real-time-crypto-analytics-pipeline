# Real-Time Crypto Market Analytics Pipeline

## Project Overview

This project is a real-time data engineering pipeline that ingests live cryptocurrency price data from the CoinGecko API, streams it through Kafka, stores it in ClickHouse, transforms it using dbt, and orchestrates the analytical workflow using Apache Airflow.

The goal of this project is to demonstrate a production-style streaming analytics architecture with:

- API-based data ingestion
- Kafka-based event streaming
- Dockerized producer and consumer services
- ClickHouse analytical storage
- RAW, STAGING, and MART data layers
- dbt incremental transformations
- deduplication and replay-safe processing
- Airflow orchestration
- producer and consumer observability
- failed event tracking
- Docker Compose-based local infrastructure

---

## Tech Stack

| Component | Technology |
|---|---|
| API Source | CoinGecko API |
| Streaming Platform | Apache Kafka |
| Producer | Python |
| Consumer | Python |
| Analytical Database | ClickHouse |
| Transformation Tool | dbt |
| Orchestration | Apache Airflow |
| Containerization | Docker Compose |
| Monitoring | Custom metrics tables + dbt models |

---

## High-Level Architecture

```text
CoinGecko API
    ↓
Python Kafka Producer
    ↓
Kafka Topic: crypto_prices
    ↓
Python Kafka Consumer
    ↓
ClickHouse RAW Layer
    ↓
dbt STAGING Layer
    ↓
dbt MART Layer
    ↓
Airflow Orchestration + dbt Tests
```
---

## For the detailed architecture diagram, see:

architecture/project_architecture.md

---

