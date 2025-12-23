SELECT
    id,
    current_price
FROM {{ source('crypto_raw', 'top_cryptos_raw') }}
WHERE current_price < 0