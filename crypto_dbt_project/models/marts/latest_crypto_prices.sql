{{ config(
    materialized='table'
) }}

WITH normalized_prices AS (

    SELECT
        'bitcoin' AS coin_id,
        bitcoin_price AS price_usd,
        event_timestamp,
        ingestion_timestamp
    FROM {{ ref('stg_crypto_prices') }}

    UNION ALL

    SELECT
        'ethereum' AS coin_id,
        ethereum_price AS price_usd,
        event_timestamp,
        ingestion_timestamp
    FROM {{ ref('stg_crypto_prices') }}

),

ranked_prices AS (

    SELECT
        *,
        row_number() OVER (
            PARTITION BY coin_id
            ORDER BY event_timestamp DESC, ingestion_timestamp DESC
        ) AS rn
    FROM normalized_prices

)

SELECT
    coin_id,
    price_usd,
    event_timestamp,
    ingestion_timestamp

FROM ranked_prices
WHERE rn = 1