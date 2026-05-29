flowchart TD

    A[CoinGecko API] --> B[Python Kafka Producer<br/>Docker Container]

    B --> C[Kafka Topic<br/>crypto_prices]

    C --> D[Python Kafka Consumer<br/>Docker Container]

    D --> E[ClickHouse RAW Layer<br/>raw_crypto_prices]

    E --> F[dbt Staging Layer<br/>stg_crypto_prices<br/>Deduplication + Validation]

    F --> G[dbt Mart Layer<br/>crypto_price_metrics<br/>latest_crypto_prices]

    H[Airflow DAG<br/>crypto_analytics_pipeline] --> I[dbt run]
    I --> J[dbt test]

    I --> F
    I --> G
    J --> F
    J --> G

    B --> K[Producer Metrics<br/>producer_api_metrics]
    D --> L[Consumer Metrics<br/>consumer_processing_metrics]
    D --> M[Failed Events<br/>failed_crypto_events]

    K --> N[Pipeline Health Summary<br/>pipeline_health_summary]
    L --> N
    M --> N

    O[Docker Compose] --> B
    O --> C
    O --> D
    O --> E
    O --> H

    P[.env Configuration] --> O
    Q[Persistent Volumes] --> E
    Q --> H

## Architecture Explanation

This project implements a real-time crypto market analytics pipeline using a streaming-first architecture.

The pipeline starts with a Python producer that calls the CoinGecko API every minute and publishes crypto price events into a Kafka topic named `crypto_prices`.

A Python Kafka consumer continuously reads messages from Kafka, validates the event structure, and writes valid events into the ClickHouse RAW layer. Invalid or malformed events are stored separately in `failed_crypto_events` for debugging and observability.

ClickHouse acts as the analytical database. The RAW layer stores event data as received, while dbt builds the STAGING and MART layers. The staging model performs validation and deduplication using event-level logic, while mart models create analytics-ready tables such as historical price metrics and latest-state crypto prices.

Airflow orchestrates dbt transformations and tests on a schedule. The Kafka producer and consumer run continuously as Docker-managed services, while Airflow periodically runs `dbt run` and `dbt test`.

The project also includes observability features such as producer metrics, consumer metrics, failed-event tracking, and a pipeline health summary model. Docker Compose manages the full local infrastructure, including Kafka, ClickHouse, Airflow, producer, consumer, persistent volumes, health checks, topic initialization, and environment-based configuration.