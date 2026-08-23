-- Fails when the dimension is empty: guards against max(dt) over a table
-- with no partitions silently producing zero rows.
select n
from (
    select count(*) as n
    from {{ ref('dim_contracts') }}
)
where n = 0
