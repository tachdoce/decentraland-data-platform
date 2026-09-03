-- Fails when (chain_id, contract_address) is not unique in the dimension.
SELECT
    chain_id,
    contract_address,
    COUNT(*) AS n
FROM {{ ref('dim_erc20_tokens') }}
GROUP BY chain_id, contract_address
HAVING COUNT(*) > 1
