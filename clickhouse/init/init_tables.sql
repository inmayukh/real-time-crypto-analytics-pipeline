CREATE DATABASE IF NOT EXISTS crypto_analytics;

--------------------------------------------------
-- RAW EVENTS TABLE
--------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_analytics.raw_crypto_prices
(
    event_id String,
    event_timestamp DateTime,
    ingestion_timestamp DateTime DEFAULT now(),
    source String,
    bitcoin_price Float64,
    ethereum_price Float64
)
ENGINE = MergeTree()
ORDER BY event_timestamp;

--------------------------------------------------
-- FAILED EVENTS TABLE
--------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_analytics.failed_crypto_events
(
    event_id String,
    error_message String,
    raw_event String,
    failed_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY failed_at;

--------------------------------------------------
-- CONSUMER METRICS TABLE
--------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_analytics.consumer_processing_metrics
(
    metric_timestamp DateTime,
    events_processed UInt64,
    events_inserted UInt64,
    invalid_events UInt64,
    failed_events UInt64,
    status String
)
ENGINE = MergeTree()
ORDER BY metric_timestamp;

--------------------------------------------------
-- PRODUCER METRICS TABLE
--------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_analytics.producer_api_metrics
(
    metric_timestamp DateTime,
    api_calls UInt64,
    successful_api_calls UInt64,
    rate_limit_errors UInt64,
    events_produced UInt64,
    failed_api_calls UInt64,
    status String
)
ENGINE = MergeTree()
ORDER BY metric_timestamp;

CREATE TABLE IF NOT EXISTS crypto_analytics.raw_crypto_ticker_events
(
    event_id String,
    event_timestamp DateTime,
    ingestion_timestamp DateTime DEFAULT now(),
    source String,
    symbol String,
    base_asset String,
    quote_asset String,
    price Float64,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    volume Float64,
    quote_volume Float64
)
ENGINE = MergeTree()
ORDER BY (symbol, event_timestamp);
-- Retention policies
ALTER TABLE crypto_analytics.raw_crypto_ticker_events MODIFY TTL ingestion_timestamp + INTERVAL 1 DAY;
ALTER TABLE crypto_analytics.stg_crypto_ticker_events MODIFY TTL ingestion_timestamp + INTERVAL 3 DAY;
ALTER TABLE crypto_analytics.failed_crypto_events MODIFY TTL failed_at + INTERVAL 3 DAY;
ALTER TABLE crypto_analytics.consumer_processing_metrics MODIFY TTL metric_timestamp + INTERVAL 3 DAY;
