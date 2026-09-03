-- Fails when the model is empty: guards against an empty bronze table or a
-- broken join silently producing zero rows.
SELECT n
FROM (
    SELECT COUNT(*) AS n
    FROM {{ ref('token_prices') }}
)
WHERE n = 0
