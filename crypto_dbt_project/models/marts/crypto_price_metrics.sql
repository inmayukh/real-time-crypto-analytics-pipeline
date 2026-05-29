{{ config(
    materialized='incremental',
    unique_key='metric_timestamp'
) }}

SELECT
    event_timestamp AS metric_timestamp,
    bitcoin_price,
    ethereum_price,
    bitcoin_price - ethereum_price AS price_difference,
    ingestion_timestamp

FROM {{ ref('stg_crypto_prices') }}

{% if is_incremental() %}
WHERE ingestion_timestamp > (
    SELECT max(ingestion_timestamp)
    FROM {{ this }}
)
{% endif %}