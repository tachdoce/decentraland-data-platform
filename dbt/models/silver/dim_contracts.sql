-- Curated contract dimension. Single source of truth: bronze.contracts
-- (the hand-curated CSV). The official registry (bronze.dcl_contracts) is
-- deliberately NOT merged here — its daily diff only alerts a human to
-- update the CSV. Grain: one row per (chain_id, contract_address).

with latest as (

    select max(dt) as dt
    from {{ source('bronze', 'contracts') }}

)

select
    c.chain_id,
    c.contract_address,
    c.contract_name,
    c.dcl_contract,
    c.erc_type,
    c.first_mint_dt,
    c.extract_from_dt,
    c.dt as snapshot_dt
from {{ source('bronze', 'contracts') }} c
inner join latest on c.dt = latest.dt
where c.erc_type != -1
