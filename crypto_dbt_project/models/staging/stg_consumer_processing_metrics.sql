{{ config(materialized='view') }}

SELECT
    metric_timestamp,
    events_processed,
    events_inserted,
    invalid_events,
    failed_events,
    status

FROM {{ source('raw', 'consumer_processing_metrics') }}