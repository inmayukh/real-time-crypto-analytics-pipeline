{{ config(materialized='view') }}
WITH consumer_health AS (
    SELECT
        max(metric_timestamp) AS latest_consumer_metric_time,
        max(events_processed) AS events_processed,
        max(events_inserted) AS events_inserted,
        max(invalid_events) AS invalid_events,
        max(failed_events) AS failed_events
    FROM {{ ref('stg_consumer_processing_metrics') }}
)
SELECT
    c.latest_consumer_metric_time,
    c.events_processed,
    c.events_inserted,
    c.invalid_events,
    c.failed_events
FROM consumer_health c
