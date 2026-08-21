-- Contract changes between the latest snapshot and the previous one.
-- Output contract (consumed by the future dbt-Lambda notifier):
--   one row per change, columns: change_type ('added'|'removed'|'renamed'),
--   chain_id, contract_address, dt (snapshot date of the latest side).
-- Empty result = no changes = no notification.

with dim_contracts as (

    select *
    from {{ ref('dim_contracts') }}

)

-- TODO(user): change-detection logic goes here.
select
    cast(null as varchar) as change_type,
    chain_id,
    contract_address,
    cast(null as varchar) as dt
from dim_contracts
where false
