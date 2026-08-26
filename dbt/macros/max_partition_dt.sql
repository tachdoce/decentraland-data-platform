-- Latest dt of a partitioned table, read from Glue "$partitions" metadata:
-- zero bytes scanned, unlike MAX(dt) over the data table. Views cannot
-- reference $partitions, so callers must be materialized as table.
{% macro max_partition_dt(relation) %}
    (
        SELECT MAX(dt)
        FROM "{{ relation.schema }}"."{{ relation.identifier }}$partitions"
    )
{% endmacro %}
