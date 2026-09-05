-- MANA transfers fact for the gold star schema. Grain: one row per
-- (chain_id, transaction_hash, log_index) — unique, ERC-20 Transfer
-- emits one row per log. Single-token fact: no token_address, no
-- surrogate key, decimals = 18 hardcoded in the conversion below.
--
-- amount converts wei exactly: DECIMAL '0.000000000000000001' is
-- decimal(18,18) and represents 1e-18 exactly; decimal multiplication
-- adds scales (38,0 x 18,18 -> scale 18, precision capped 38), so no
-- value ever routes through double (POW(10,18) would, losing wei-level
-- digits). The CAST to decimal(30,18) keeps scale 18 intact and leaves
-- 12 integer digits — MANA supply (~2,193M) is ~456x below that limit.
--
-- No dedup here: silver.mana_transfers already keeps one copy per
-- grain (dual decoded_at + bronze_extracted_at dedup, insert_overwrite
-- partitions, uniqueness test). amount_raw = 0 no-op transfers stay in
-- silver but are dropped from gold, matching fct_nft_transfers; mints
-- and burns (zero address) are kept — real supply movements.
--
-- The initial build seeds dt < '2019-01-01' (data starts 2017-09-06);
-- incremental runs advance at most mana_transfers_incremental_days
-- (default 10, same var and cadence as silver) past the current max
-- dt, e.g. --vars '{mana_transfers_incremental_days: 100}' to catch up.
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
    CAST(t.amount_raw * DECIMAL '0.000000000000000001' AS decimal(30, 18)) AS amount,
    t.chain_id,
    t.silver_processed_at,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    t.dt
FROM {{ ref('mana_transfers') }} AS t
WHERE t.amount_raw > 0
{% if is_incremental() %}
  AND t.dt > {{ max_partition_dt(this) }}
  AND t.dt <= {{ max_partition_dt_offset(this, var('mana_transfers_incremental_days', 10)) }}
{% else %}
  AND t.dt < '2019-01-01'
{% endif %}
