-- Payment-currency dimension for the gold star schema. No surrogate key:
-- facts join prices by currency_symbol, since one currency can live on N
-- chains with the same quote. Grain: one row per (chain_id, contract_address).

SELECT
    chain_id,
    contract_address,
    name,
    fsym AS currency_symbol,
    decimals,
    fetch_price
FROM {{ ref('dim_erc20_tokens') }}
