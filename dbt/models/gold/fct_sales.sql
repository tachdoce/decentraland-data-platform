-- Sales fact for the gold star schema. One row per token sold, keyed by
-- (chain_id, transaction_hash, log_index, sk_contract, token_id).
-- INNER JOINs are intentional: only sales whose payment token is curated
-- in dim_currency AND has a USD quote for the sale day (token_prices is
-- gap-filled up to price_fill_end_dt) make it into gold. Addresses travel
-- via sk_contract / currency_symbol; join the dims to recover them.
--
-- No dedup here: silver.sales already keeps one copy per grain (latest
-- decoded_at + bronze_extracted_at per staging scan, insert_overwrite
-- partitions) and its unique-key test guards it. Loading recipe mirrors
-- silver.sales, but starting at 2019-01-01 — the first day token_prices
-- covers (DefiLlama backfill grid); 2018 sales stay silver-only. Initial
-- build seeds January 2019; incremental runs advance at most
-- sales_incremental_days (default 10) past max dt, e.g.
-- --vars '{sales_incremental_days: 90}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['dt']
) }}

-- The window goes in a CTE: the macro emits an unqualified dt, ambiguous
-- once token_prices (also carrying dt) joins in.
WITH sales_window AS (
    SELECT *
    FROM {{ ref('sales') }}
    WHERE {{ sales_dt_window('2019-01-01', seed_end_dt='2019-02-01') }}
)

SELECT s.transaction_hash,
    s.log_index,
    s.block_timestamp,
    s.buyer,
    s.seller,
    n.sk_contract,
    s.token_id,
    s.quantity,
    c.currency_symbol,
    ROUND(s.total_amount_raw / POW(10, c.decimals), 6) AS total_amount,
    ROUND(COALESCE(s.royalty_amount_raw, 0) / POW(10, c.decimals), 6)
        AS royalty_amount,
    ROUND(s.total_amount_raw / POW(10, c.decimals) * p.price_usd, 6)
        AS usd_total_amount,
    ROUND(COALESCE(s.royalty_amount_raw, 0) / POW(10, c.decimals) * p.price_usd, 6)
        AS usd_royalty_amount,
    s.chain_id,
    s.marketplace,
    s.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    s.dt
FROM sales_window AS s
    INNER JOIN {{ ref('dim_currency') }} AS c
        ON s.chain_id = c.chain_id
        AND s.currency = c.contract_address
    INNER JOIN {{ ref('token_prices') }} AS p
        ON c.currency_symbol = p.currency_symbol
        AND s.dt = p.dt
    INNER JOIN {{ ref('dim_nft_contracts') }} AS n
        ON s.chain_id = n.chain_id
        AND s.nft_contract_address = n.contract_address
