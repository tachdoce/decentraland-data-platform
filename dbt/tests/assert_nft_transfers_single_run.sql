-- After dedup, every log must come from exactly one extraction run.
-- Classic uniqueness does not apply: a 1155 TransferBatch legitimately
-- yields many rows per (transaction_hash, log_index).
SELECT chain_id,
    transaction_hash,
    log_index
FROM {{ ref('nft_transfers') }}
GROUP BY chain_id,
    transaction_hash,
    log_index
HAVING COUNT(DISTINCT bronze_extracted_at) > 1
