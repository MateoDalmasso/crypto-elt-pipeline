SELECT
    crypto_id,
    total_volume
FROM {{ ref('stg_crypto_prices') }}
WHERE total_volume < 0