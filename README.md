# Real-Time Crypto Market Analytics Pipeline

![Architecture Diagram](../AppData/Local/Temp/096e2b9a-b279-4b04-b847-5b65022e7463_project2_github_polish_files.zip.463/architecture/architecture.png)

## Project Overview

This project is a Docker-managed real-time data engineering pipeline that ingests live cryptocurrency price data from the CoinGecko API, streams it through Kafka, stores it in ClickHouse, transforms it using dbt, and orchestrates the analytical workflow using Apache Airflow.

The project is designed to demonstrate production-style streaming data engineering concepts such as API ingestion, Kafka event streaming, replay-safe processing, dbt incremental models, CDC-style latest-state modeling, observability, failed-event handling, Docker Compose infrastructure, persistent storage, health checks, and environment-based configuration.

---

## Why This Project

The goal was to move beyond a simple batch ETL project and build a more realistic streaming analytics platform.

This project answers questions such as:

- How do we ingest frequently changing API data?
- How do we decouple ingestion from processing using Kafka?
- How do we handle malformed API responses and rate limits?
- How do we make Kafka ingestion replay-safe?
- How do we build RAW, STAGING, and MART layers on streaming data?
- How do we monitor producer and consumer health?
- How do we orchestrate transformations and tests with Airflow?
- How do we make the project reproducible using Docker Compose?

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Source | CoinGecko API |
| Streaming | Apache Kafka |
| Producer | Python |
| Consumer | Python |
| Analytical Database | ClickHouse |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Infrastructure | Docker Compose |
| Configuration | `.env` |
| Observability | Metrics tables + dbt monitoring models |

---

## High-Level Architecture

```text
CoinGecko API
    ↓
Dockerized Python Kafka Producer
    ↓
Kafka Topic: crypto_prices
    ↓
Dockerized Python Kafka Consumer
    ↓
ClickHouse RAW Layer
    ↓
dbt STAGING Layer
    ↓
dbt MART Layer
    ↓
Airflow Orchestration + dbt Tests
```

Detailed architecture is also documented in:

```text
architecture/project_architecture.md
```

---

## Project Folder Structure

```text
Real Time Crypto Analytics Pipeline
│
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py
│
├── architecture/
│   ├── architecture.png
│   └── project_architecture.md
│
├── clickhouse/
│   └── init/
│       └── init_tables.sql
│
├── crypto_dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── monitoring/
│   ├── profiles/
│   └── dbt_project.yml
│
├── ingestion/
│   ├── crypto_producer.py
│   └── crypto_consumer.py
│
├── screenshots/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Data Flow

### 1. API Ingestion

The producer calls the CoinGecko API every minute and fetches Bitcoin and Ethereum price data.

It validates the API response before publishing to Kafka. API rate-limit responses and invalid payloads are handled safely.

### 2. Kafka Streaming

Kafka acts as the event streaming layer.

The topic used is:

```text
crypto_prices
```

The topic is created automatically by the `kafka_init` service in Docker Compose.

### 3. Consumer Processing

The consumer reads events from Kafka, validates the message structure, and inserts valid records into ClickHouse.

Invalid events are written to:

```text
failed_crypto_events
```

### 4. ClickHouse RAW Layer

The RAW table stores ingested streaming events:

```text
raw_crypto_prices
```

This table is append-oriented and helps with replay, auditability, and debugging.

### 5. dbt STAGING Layer

The staging model cleans, validates, and deduplicates raw events:

```text
stg_crypto_prices
```

This layer handles replay-safe processing using event-level deduplication logic.

### 6. dbt MART Layer

The mart layer creates analytics-ready tables:

```text
crypto_price_metrics
latest_crypto_prices
```

These models support historical analysis and latest-state lookup.

### 7. Observability Layer

The project includes monitoring tables:

```text
producer_api_metrics
consumer_processing_metrics
failed_crypto_events
pipeline_health_summary
```

These help track producer behavior, consumer behavior, invalid events, and overall pipeline health.

---

## Key Features

- Real-time API ingestion
- Kafka producer and consumer architecture
- Dockerized streaming services
- ClickHouse analytical storage
- RAW, STAGING, and MART layers
- dbt incremental transformations
- Replay-safe deduplication
- CDC-style latest-state model
- Airflow orchestration
- dbt tests
- Producer and consumer metrics
- Failed-event logging
- Kafka topic auto-creation
- Docker health checks
- Persistent ClickHouse and Airflow volumes
- `.env`-based configuration
- Structured Python logging
- Retry handling for service readiness

---

## Setup Instructions

### Prerequisites

Install:

- Docker Desktop
- Git
- Python 3.11+
- VS Code or another editor

---

## Environment Variables

Create a `.env` file in the project root using `.env.example`.

Example:

```env
CLICKHOUSE_DB=crypto_analytics
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=admin123

AIRFLOW_USER=airflow
AIRFLOW_PASSWORD=airflow
```

Do not commit `.env` to GitHub.

---

## Run the Project

From the project root:

```powershell
docker compose up -d --build
```

This starts:

- Zookeeper
- Kafka
- Kafka topic initialization service
- ClickHouse
- Airflow
- Crypto producer
- Crypto consumer

---

## Verify Running Containers

```powershell
docker compose ps
```

Expected services:

```text
zookeeper
kafka
clickhouse
airflow
crypto_producer
crypto_consumer
```

The `kafka_init` container may exit after successfully creating the topic.

---

## Verify Kafka Topic

```powershell
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Expected:

```text
crypto_prices
```

---

## Verify Producer

```powershell
docker logs crypto_producer --tail 50
```

Expected log pattern:

```text
Produced event to Kafka
```

---

## Verify Consumer

```powershell
docker logs crypto_consumer --tail 50
```

Expected log pattern:

```text
Inserted event into ClickHouse
```

---

## Access ClickHouse

```powershell
docker exec -it clickhouse clickhouse-client --user admin --password admin123
```

Check raw event count:

```sql
SELECT count(*)
FROM crypto_analytics.raw_crypto_prices;
```

View latest events:

```sql
SELECT *
FROM crypto_analytics.raw_crypto_prices
ORDER BY ingestion_timestamp DESC
LIMIT 5;
```

---

## Access Airflow

Open:

```text
http://localhost:8081
```

Credentials:

```text
Username: airflow
Password: airflow
```

Trigger the DAG:

```text
crypto_analytics_pipeline
```

The DAG runs:

```text
dbt run
dbt test
```

---

## dbt Models

### Sources

```text
raw_crypto_prices
failed_crypto_events
producer_api_metrics
consumer_processing_metrics
```

### Staging Models

```text
stg_crypto_prices
stg_producer_api_metrics
stg_consumer_processing_metrics
stg_failed_crypto_events
```

### Mart / Monitoring Models

```text
crypto_price_metrics
latest_crypto_prices
pipeline_health_summary
```

---

## Running dbt Manually

```powershell
cd crypto_dbt_project
dbt run --profiles-dir profiles
dbt test --profiles-dir profiles
```

---

## Important Queries

### Latest Raw Events

```sql
SELECT *
FROM crypto_analytics.raw_crypto_prices
ORDER BY ingestion_timestamp DESC
LIMIT 10;
```

### Latest Crypto Prices

```sql
SELECT *
FROM crypto_analytics.latest_crypto_prices;
```

### Historical Metrics

```sql
SELECT *
FROM crypto_analytics.crypto_price_metrics
ORDER BY ingestion_timestamp DESC
LIMIT 10;
```

### Producer Monitoring

```sql
SELECT *
FROM crypto_analytics.producer_api_metrics
ORDER BY metric_timestamp DESC
LIMIT 10;
```

### Consumer Monitoring

```sql
SELECT *
FROM crypto_analytics.consumer_processing_metrics
ORDER BY metric_timestamp DESC
LIMIT 10;
```

### Pipeline Health Summary

```sql
SELECT *
FROM crypto_analytics.pipeline_health_summary;
```

---

## Screenshots

Add screenshots in the `screenshots/` folder and reference them here.

Suggested screenshots:

```text
screenshots/docker-compose-running.png
screenshots/airflow-dag-success.png
screenshots/clickhouse-raw-events.png
screenshots/dbt-test-success.png
screenshots/pipeline-health-summary.png
```

Example Markdown:

```markdown
![Airflow DAG Success](screenshots/airflow-dag-success.png)
```

---

## Production Engineering Concepts Demonstrated

This project demonstrates:

- event-driven architecture
- Kafka producer and consumer patterns
- API ingestion
- consumer groups and offsets
- at-least-once delivery behavior
- duplicate handling
- replay-safe transformations
- idempotent processing design
- dbt incremental modeling
- CDC-style latest-state modeling
- Airflow orchestration
- data quality testing
- failed-event capture
- observability tables
- Dockerized services
- persistent volumes
- health checks
- environment-based configuration
- structured logging
- startup retry handling

---

## Known Limitations

- CoinGecko free API can rate-limit requests.
- The pipeline tracks only Bitcoin and Ethereum.
- This is a local Docker Compose project, not a cloud deployment.
- Airflow uses containerized local metadata persistence instead of an external PostgreSQL metadata database.
- No real Slack/email alerting is configured yet.
- No dashboarding layer is included yet.

---

## Future Improvements

- Add PostgreSQL as Airflow metadata database.
- Add Grafana, Superset, or Metabase dashboards.
- Add Slack/email alerts for Airflow failures.
- Add more crypto assets.
- Add ClickHouse partitioning and retention policies.
- Add schema validation for incoming events.
- Add CI/CD checks for dbt models.
- Add a real CDC pipeline using PostgreSQL + Debezium + Kafka.

---

## Interview Summary

This project is a real-time data engineering pipeline that ingests live crypto market data from an external API, streams events through Kafka, stores data in ClickHouse, transforms it using dbt, and orchestrates dbt workflows through Airflow.

It demonstrates practical data engineering concepts including streaming ingestion, event validation, Kafka replay behavior, incremental models, replay-safe deduplication, CDC-style latest-state modeling, observability, failed-event handling, Dockerized infrastructure, and production-style orchestration.

---

## Author

Mayukh Chowdhury
