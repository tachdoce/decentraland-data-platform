-- NFT transfers fact for the gold star schema. Grain: one row per
-- transferred token position — a 1155 TransferBatch explodes into one
-- row per array element, so (chain_id, transaction_hash, log_index)
-- can legitimately repeat, even with identical token_id. The INNER
-- JOIN to dim_nft_contracts is intentional: only ERC-721/1155 curated
-- contracts make it into gold (EstateProxy, erc_type = 0, drops out).
-- Addresses travel via sk_contract; join the dim to recover them.
--
-- No dedup here: silver.nft_transfers already keeps one copy per log
-- and run (latest bronze_extracted_at, insert_overwrite partitions).
-- Partitioned by dt only (chain_id is a regular column), so one dt
-- overwrite covers every chain once the polygon decode lands.
--
-- The initial build seeds dt < '2019-01-01' (no price dependency,
-- unlike fct_sales); incremental runs advance at most
-- nft_transfers_incremental_days (default 10, same var and cadence as
-- silver) past the current max dt, e.g.
-- --vars '{nft_transfers_incremental_days: 100}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['dt']
) }}

SELECT t.transaction_hash,
    t.log_index,
    t.block_timestamp,
    t.from_address,
    t.to_address,
    c.sk_contract,
    t.token_id,
    t.quantity,
    t.chain_id,
    t.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    t.dt
FROM {{ ref('nft_transfers') }} AS t
    INNER JOIN {{ ref('dim_nft_contracts') }} AS c
        ON t.chain_id = c.chain_id
        AND t.contract_address = c.contract_address
{% if is_incremental() %}
WHERE t.dt > {{ max_partition_dt(this) }}
  AND t.dt <= {{ max_partition_dt_offset(this, var('nft_transfers_incremental_days', 10)) }}
{% else %}
WHERE t.dt < '2019-01-01'
{% endif %}
