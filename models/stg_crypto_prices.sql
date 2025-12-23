WITH raw_data AS (
    SELECT * FROM {{ source('crypto_raw', 'top_cryptos_raw') }}
)

SELECT
    id AS crypto_id,
    symbol AS ticker,
    name AS crypto_name,
    current_price AS price_usd,
    market_cap,
    total_volume,
    CAST(last_updated AS TIMESTAMP) AS last_updated_at,
    CAST(extracted_at AS TIMESTAMP) AS data_load_at
FROM raw_data