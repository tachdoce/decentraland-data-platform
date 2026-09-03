-- Daily USD price per currency symbol, gap-filled. Grain: one row per
-- (currency_symbol, dt). Sources: bronze.token_prices (append-only, deduped
-- here by latest extracted_at) joined to dim_erc20_tokens.
--
-- Gap-filling: each token gets a calendar spine from its first priced day
-- to var('price_fill_end_dt'); days without a quote carry the last known
-- price forward (is_price_filled marks them).
--
-- Symbol grain is safe because the dimension governs the universe:
-- fetch_price = FALSE excludes duplicate-symbol contracts (GALA v1) and
-- anything the curator turns off.
{{ config(materialized='table') }}

WITH deduped AS (
    SELECT
        p.chain_id,
        p.contract_address,
        p.dt,
        p.price_usd,
        ROW_NUMBER() OVER (
            PARTITION BY p.chain_id, p.contract_address, p.dt
            ORDER BY p.extracted_at DESC
        ) AS rn
    FROM {{ source('bronze', 'token_prices') }} AS p
),

bounds AS (
    SELECT
        chain_id,
        contract_address,
        MIN(dt) AS first_dt,
        DATE '{{ var("price_fill_end_dt", "2026-08-20") }}' AS last_dt
    FROM deduped
    WHERE rn = 1
    GROUP BY chain_id, contract_address
),

spine AS (
    SELECT
        b.chain_id,
        b.contract_address,
        cal.dt
    FROM bounds AS b
    CROSS JOIN UNNEST(SEQUENCE(b.first_dt, b.last_dt, INTERVAL '1' DAY)) AS cal (dt)
),

filled AS (
    SELECT
        s.chain_id,
        s.contract_address,
        s.dt,
        LAST_VALUE(d.price_usd) IGNORE NULLS OVER (
            PARTITION BY s.chain_id, s.contract_address
            ORDER BY s.dt
        ) AS price_usd,
        (d.price_usd IS NULL) AS is_price_filled
    FROM spine AS s
    LEFT JOIN deduped AS d
        ON d.chain_id = s.chain_id
        AND d.contract_address = s.contract_address
        AND d.dt = s.dt
        AND d.rn = 1
)

SELECT
    e.fsym AS currency_symbol,
    f.price_usd,
    -- SEQUENCE over dates yields timestamps; back to date before varchar
    -- so dt is a clean YYYY-MM-DD string
    CAST(CAST(f.dt AS date) AS varchar) AS dt
FROM filled AS f
INNER JOIN {{ ref('dim_erc20_tokens') }} AS e
    ON e.chain_id = f.chain_id
    AND e.contract_address = f.contract_address
WHERE e.fetch_price = TRUE
