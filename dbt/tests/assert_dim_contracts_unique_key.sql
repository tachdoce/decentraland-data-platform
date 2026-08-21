-- Composite uniqueness on (chain_id, contract_address) without dbt_utils.
-- Fails if any key appears more than once.

select
    chain_id,
    contract_address,
    count(*) as n
from {{ ref('dim_contracts') }}
group by chain_id, contract_address
having count(*) > 1
