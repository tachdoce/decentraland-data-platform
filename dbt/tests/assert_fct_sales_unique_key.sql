-- The declared grain must be unique. Bounded to the window the current
-- run just loaded (max dt minus the incremental cap): an unbounded scan
-- of the full table would blow the workgroup cap; older partitions were
-- validated when loaded (nft_transfers lesson).
SELECT chain_id,
    transaction_hash,
    log_index,
    sk_contract,
    token_id
FROM {{ ref('fct_sales') }}
WHERE dt > {{ max_partition_dt_offset(ref('fct_sales'), -1 * var('sales_incremental_days', 10)) }}
GROUP BY chain_id,
    transaction_hash,
    log_index,
    sk_contract,
    token_id
HAVING COUNT(*) > 1
