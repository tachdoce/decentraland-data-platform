-- Grain guard: one row per (transaction_hash, log_index). Duplicates mean
-- either a decode bug or a bronze day re-extracted after being loaded.
SELECT
    transaction_hash,
    log_index,
    COUNT(*) AS n
FROM {{ ref('ethereum_currency_transfers') }}
GROUP BY transaction_hash, log_index
HAVING COUNT(*) > 1
