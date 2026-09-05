-- NFT contract dimension for the gold star schema: curated contracts
-- restricted to NFT standards (ERC-721/1155). Grain: one row per
-- (chain_id, contract_address), keyed by sk_contract.

SELECT
    {{ dbt_utils.generate_surrogate_key(['chain_id', 'contract_address']) }} AS sk_contract,
    chain_id,
    contract_address,
    contract_name,
    dcl_contract,
    erc_type
FROM {{ ref('dim_contracts') }}
WHERE erc_type IN (721, 1155)
