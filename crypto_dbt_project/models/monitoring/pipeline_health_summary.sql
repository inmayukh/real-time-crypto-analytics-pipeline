{{ config(materialized='view') }}

WITH producer_health AS (

    SELECT
        max(metric_timestamp) AS latest_producer_metric_time,
        max(api_calls) AS total_api_calls,
        max(successful_api_calls) AS successful_api_calls,
        max(rate_limit_errors) AS rate_limit_errors,
        max(events_produced) AS events_produced,
        max(failed_api_calls) AS failed_api_calls
    FROM {{ ref('stg_producer_api_metrics') }}

),

consumer_health AS (

    SELECT
        max(metric_timestamp) AS latest_consumer_metric_time,
        max(events_processed) AS events_processed,
        max(events_inserted) AS events_inserted,
        max(invalid_events) AS invalid_events,
        max(failed_events) AS failed_events
    FROM {{ ref('stg_consumer_processing_metrics') }}

)

SELECT
    p.latest_producer_metric_time,
    c.latest_consumer_metric_time,

    p.total_api_calls,
    p.successful_api_calls,
    p.rate_limit_errors,
    p.events_produced,
    p.failed_api_calls,

    c.events_processed,
    c.events_inserted,
    c.invalid_events,
    c.failed_events

FROM producer_health p
CROSS JOIN consumer_health c