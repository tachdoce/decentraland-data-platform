-- MANA (ERC-20) Transfer events on Ethereum, deduplicated to the latest
-- decode and bronze extraction run per log. Grain: one row per
-- (chain_id, transaction_hash, log_index) — unique, unlike 1155 batches.
--
-- Dedup is dual on decoded_at AND bronze_extracted_at: staging is
-- append-only and one decode run can ingest several bronze copies of
-- the same log (retry storms), so decoded_at alone cannot separate
-- them (same criterion as silver.sales).
--
-- amount_raw = 0 rows (legal no-op transfers) are kept: silver stays
-- faithful to chain data; filtering belongs to gold.
--
-- The initial build seeds dt < '2019-01-01' (data starts 2017-09-06,
-- MANA deployed 2017-08); incremental runs advance at most
-- mana_transfers_incremental_days (default 10) past the current max
-- dt, e.g. --vars '{mana_transfers_incremental_days: 100}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['chain_id', 'dt']
) }}

WITH ranked AS (
    SELECT
        t.transaction_hash,
        t.log_index,
        t.block_timestamp,
        t.token_address,
        t.from_address,
        t.to_address,
        t.amount_raw,
        t.bronze_extracted_at,
        t.decoded_at,
        t.dt,
        MAX(t.decoded_at) OVER (
            PARTITION BY t.dt, t.transaction_hash, t.log_index
        ) AS latest_decoded_at,
        MAX(t.bronze_extracted_at) OVER (
            PARTITION BY t.dt, t.transaction_hash, t.log_index
        ) AS latest_bronze_extracted_at
    FROM {{ source('staging', 'ethereum_currency_transfers') }} AS t
    -- MANAToken
    WHERE t.token_address = '0x0f5d2fb29fb7d3cfee444a200298f468908cc942'
    {% if is_incremental() %}
      AND t.dt > {{ max_partition_dt(this) }}
      AND t.dt <= {{ max_partition_dt_offset(this, var('mana_transfers_incremental_days', 10)) }}
    {% else %}
      AND t.dt < '2019-01-01'
    {% endif %}
)

SELECT
    transaction_hash,
    log_index,
    block_timestamp,
    token_address,
    from_address,
    to_address,
    amount_raw,
    bronze_extracted_at,
    decoded_at,
    CAST(current_timestamp AS timestamp) AS silver_processed_at,
    1 AS chain_id,
    dt
FROM ranked
WHERE decoded_at = latest_decoded_at
    AND bronze_extracted_at = latest_bronze_extracted_at
