-- ERC-20 Transfer events decoded straight from bronze in SQL (no decode
-- Lambda for this entity). No join to dim_contracts: untracked payment
-- tokens are kept on purpose for future sales parsing.
-- Grain: one row per (transaction_hash, log_index).
--
-- Incremental append: each run loads the next 20-day window after the
-- table's own max(dt), keeping every build under the workgroup's
-- bytes-scanned cap. Windows never overlap, so append is safe.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['dt']
) }}

WITH dedup AS (

    SELECT *
    FROM {{ source('bronze', 'ethereum_logs') }}
    WHERE topics[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
      AND cardinality(topics) = 3   -- ERC-721 Transfer has 4 topics
{% if is_incremental() %}
      AND dt >  {{ max_partition_dt(this) }}
      AND dt <= CAST(CAST({{ max_partition_dt(this) }} AS date) + INTERVAL '20' DAY AS varchar)
{% else %}
      -- bootstrap: the table does not exist yet, so $partitions is not
      -- available; start at MANA deployment
      AND dt <= '2017-09-06'
{% endif %}

)

SELECT
    l.transaction_hash,
    l.log_index,
    l.block_timestamp,
    l.address                              AS token_address,
    concat('0x', substr(l.topics[2], 27))  AS from_address,
    concat('0x', substr(l.topics[3], 27))  AS to_address,
    -- uint256 split into two 48-bit halves to stay inside decimal(38,0);
    -- covers amounts < 2^96 (the overflow test guards the rest)
    CAST(from_base(substr(l.data, 43, 12), 16) AS decimal(38, 0)) * DECIMAL '281474976710656'
        + CAST(from_base(substr(l.data, 55, 12), 16) AS decimal(38, 0)) AS amount_raw,
    l.extracted_at                         AS bronze_extracted_at,
    CAST(current_timestamp AS timestamp)   AS processed_at,
    l.dt
FROM dedup l
