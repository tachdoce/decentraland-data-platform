-- Fails when (chain_id, contract_address) is not unique in the dimension.
select
    chain_id,
    contract_address,
    count(*) as n
from {{ ref('dim_contracts') }}
group by chain_id, contract_address
having count(*) > 1
