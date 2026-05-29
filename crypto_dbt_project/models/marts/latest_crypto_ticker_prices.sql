{{ config(materialized='table') }}

WITH ranked_prices AS (

    SELECT
        symbol,
        base_asset,
        quote_asset,
        price,
        open_price,
        high_price,
        low_price,
        volume,
        quote_volume,
        event_timestamp,
        ingestion_timestamp,

        row_number() OVER (
            PARTITION BY symbol
            ORDER BY event_timestamp DESC, ingestion_timestamp DESC
        ) AS rn

    FROM {{ ref('stg_crypto_ticker_events') }}

)

SELECT
    symbol,
    base_asset,
    quote_asset,
    price,
    open_price,
    high_price,
    low_price,
    volume,
    quote_volume,
    event_timestamp,
    ingestion_timestamp

FROM ranked_prices
WHERE rn = 1