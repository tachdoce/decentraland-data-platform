-- Fails on non-positive or absurd prices before they reach consumers.
SELECT
    currency_symbol,
    dt,
    price_usd
FROM {{ ref('token_prices') }}
WHERE price_usd <= 0
   OR price_usd > 1e7
