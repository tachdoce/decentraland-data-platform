-- Fails when (currency_symbol, dt) is not unique: would mean two contracts
-- with the same symbol are both priced (fetch_price governance broken).
SELECT
    currency_symbol,
    dt,
    COUNT(*) AS n
FROM {{ ref('token_prices') }}
GROUP BY currency_symbol, dt
HAVING COUNT(*) > 1
