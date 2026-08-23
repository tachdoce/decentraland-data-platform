-- Unified contract dimension: fusion of the two bronze contract sources.
-- Grain: one row per (chain_id, contract_address).

with dcl_contracts as (

    select *
    from {{ source('bronze', 'dcl_contracts') }}

),

contracts as (

    select *
    from {{ source('bronze', 'contracts') }}

)

-- TODO(user): fusion logic goes here.
select
    chain_id,
    contract_address,
    contract_name
from dcl_contracts
