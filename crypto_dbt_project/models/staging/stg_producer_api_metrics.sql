{{ config(materialized='view') }}

SELECT
    metric_timestamp,
    api_calls,
    successful_api_calls,
    rate_limit_errors,
    events_produced,
    failed_api_calls,
    status

FROM {{ source('raw', 'producer_api_metrics') }}