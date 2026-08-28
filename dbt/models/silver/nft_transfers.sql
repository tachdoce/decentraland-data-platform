-- ERC-721/1155 transfers for curated contracts, unified across chains
-- (ethereum only until the polygon decode lands). Grain: one row per
-- transferred token position — a 1155 TransferBatch explodes into one
-- row per array element, so (chain_id, transaction_hash, log_index)
-- can legitimately repeat, even with identical token_id.
--
-- Dedup is per extraction run, not per row: bronze is append-only, so a
-- day extracted twice reaches staging twice under different
-- bronze_extracted_at; keeping only the latest run per log removes the
-- duplicate without collapsing legitimate batch repeats.
--
-- The initial build seeds the table up to 2018-12-31; the rest arrives
-- through incremental runs that advance at most
-- nft_transfers_incremental_days (default 10) past the current max dt,
-- e.g. --vars '{nft_transfers_incremental_days: 180}' to catch up.
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
        t.contract_address,
        t.token_id,
        t.quantity,
        t.from_address,
        t.to_address,
        t.bronze_extracted_at,
        t.decoded_at,
        t.dt,
        MAX(t.bronze_extracted_at) OVER (
            PARTITION BY t.dt, t.transaction_hash, t.log_index
        ) AS latest_bronze_extracted_at
    FROM {{ source('staging', 'ethereum_nft_transfers') }} AS t
    {% if is_incremental() %}
    WHERE t.dt > {{ max_partition_dt(this) }}
      AND t.dt <= {{ max_partition_dt_offset(this, var('nft_transfers_incremental_days', 10)) }}
    {% else %}
    WHERE t.dt < '2019-01-01'
    {% endif %}
)

SELECT
    transaction_hash,
    log_index,
    block_timestamp,
    contract_address,
    token_id,
    quantity,
    from_address,
    to_address,
    bronze_extracted_at,
    decoded_at,
    CAST(current_timestamp AS timestamp) AS silver_processed_at,
    1 AS chain_id,
    dt
FROM ranked
WHERE bronze_extracted_at = latest_bronze_extracted_at
