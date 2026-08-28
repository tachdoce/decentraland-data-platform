-- After dedup, every log must come from exactly one extraction run.
-- Classic uniqueness does not apply: a 1155 TransferBatch legitimately
-- yields many rows per (transaction_hash, log_index).
--
-- Bounded to the window the current run just loaded (max dt minus the
-- incremental cap): an unbounded scan of the full table exceeds the
-- 1 GB workgroup cap. Older partitions were validated when loaded and
-- insert_overwrite cannot corrupt them afterwards.
SELECT chain_id,
    transaction_hash,
    log_index
FROM {{ ref('nft_transfers') }}
WHERE dt > {{ max_partition_dt_offset(ref('nft_transfers'), -1 * var('nft_transfers_incremental_days', 10)) }}
GROUP BY chain_id,
    transaction_hash,
    log_index
HAVING COUNT(DISTINCT bronze_extracted_at) > 1
