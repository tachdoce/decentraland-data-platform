-- Fails when the dimension is empty: guards against max(dt) over a table
-- with no partitions silently producing zero rows.
SELECT n
FROM (
    SELECT COUNT(*) AS n
    FROM {{ ref('dim_erc20_tokens') }}
)
WHERE n = 0
