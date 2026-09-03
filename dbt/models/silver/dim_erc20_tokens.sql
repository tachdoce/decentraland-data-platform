-- Curated payment-token dimension. Single source of truth:
-- bronze.erc20_tokens (the hand-curated CSV). Grain: one row per
-- (chain_id, contract_address). fsym is informative and NOT unique
-- (GALA v1/v2 share it); joins use the natural key.
--
-- Table, not view: Athena views cannot reference $partitions, and a table
-- pins the latest snapshot at build time (the build runs after every
-- ingestion, so it never goes stale).
{{ config(materialized='table') }}

SELECT
    t.chain_id,
    t.contract_address,
    t.name,
    t.fsym,
    t.decimals,
    t.fetch_price,
    t.dt AS snapshot_dt
FROM {{ source('bronze', 'erc20_tokens') }} t
WHERE t.dt = {{ max_partition_dt(source('bronze', 'erc20_tokens')) }}
