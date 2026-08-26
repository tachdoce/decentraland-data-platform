-- The model decodes only the low 96 bits of the uint256 amount; a transfer
-- with any of the high 160 bits set would be silently truncated. Scan is
-- bounded to the 20-day window just loaded so it stays under the 1 GB cap.
SELECT
    transaction_hash,
    log_index,
    dt,
    data
FROM {{ source('bronze', 'ethereum_logs') }}
WHERE topics[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND cardinality(topics) = 3
  AND dt <= {{ max_partition_dt(ref('ethereum_currency_transfers')) }}
  AND dt >  CAST(CAST({{ max_partition_dt(ref('ethereum_currency_transfers')) }} AS date) - INTERVAL '20' DAY AS varchar)
  -- 40 hex zeros: Trino's repeat() builds arrays, so the literal it is
  AND substr(data, 3, 40) != '0000000000000000000000000000000000000000'
