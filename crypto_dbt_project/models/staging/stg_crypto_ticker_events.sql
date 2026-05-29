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
        symbol,
        base_asset,
        quote_asset,
        price,
        open_price,
        high_price,
        low_price,
        volume,
        quote_volume,

        row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_timestamp DESC
        ) AS rn

    FROM {{ source('raw', 'raw_crypto_ticker_events') }}

    WHERE price > 0
      AND open_price >= 0
      AND high_price >= 0
      AND low_price >= 0
      AND volume >= 0
      AND quote_volume >= 0
      AND symbol != ''

)

SELECT
    event_id,
    event_timestamp,
    ingestion_timestamp,
    source,
    symbol,
    base_asset,
    quote_asset,
    price,
    open_price,
    high_price,
    low_price,
    volume,
    quote_volume

FROM ranked_events
WHERE rn = 1

{% if is_incremental() %}
  AND ingestion_timestamp > (
      SELECT max(ingestion_timestamp)
      FROM {{ this }}
  )
{% endif %}