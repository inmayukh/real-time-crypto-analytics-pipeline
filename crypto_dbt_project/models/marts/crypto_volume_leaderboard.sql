{{ config(materialized='table') }}

SELECT
    symbol,
    base_asset,
    quote_asset,
    max(price) AS latest_seen_price,
    max(quote_volume) AS quote_volume,
    max(volume) AS base_volume,
    max(ingestion_timestamp) AS latest_ingestion_timestamp

FROM {{ ref('stg_crypto_ticker_events') }}

GROUP BY
    symbol,
    base_asset,
    quote_asset