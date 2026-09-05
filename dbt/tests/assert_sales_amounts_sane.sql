-- Amount sanity over the freshly loaded window; also the canary for
-- float64-style price corruption (see the 2026-09-04 marketplace v2
-- incident): corrupted prices tend to break the royalty/total relation.
SELECT marketplace,
    transaction_hash,
    log_index,
    total_amount_raw,
    royalty_amount_raw
FROM {{ ref('sales') }}
WHERE dt > {{ max_partition_dt_offset(ref('sales'), -1 * var('sales_incremental_days', 10)) }}
    AND (total_amount_raw < 0
        OR royalty_amount_raw < 0
        OR royalty_amount_raw > total_amount_raw)
