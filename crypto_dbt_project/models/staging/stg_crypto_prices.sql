{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH ranked_events AS (

    SELECT
        event_id,
        event_timestamp,
        ingestion_timestamp,
        source,
        bitcoin_price,
        ethereum_price,

        row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_timestamp DESC
        ) AS rn

    FROM {{ source('raw', 'raw_crypto_prices') }}

    WHERE bitcoin_price > 0
      AND ethereum_price > 0

)

SELECT
    event_id,
    event_timestamp,
    ingestion_timestamp,
    source,
    bitcoin_price,
    ethereum_price

FROM ranked_events
WHERE rn = 1

{% if is_incremental() %}
  AND ingestion_timestamp > (
      SELECT max(ingestion_timestamp)
      FROM {{ this }}
  )
{% endif %}