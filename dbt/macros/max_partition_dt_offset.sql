-- Latest dt of a partitioned table plus an offset in days, read from the
-- "$partitions" metadata like max_partition_dt (zero bytes scanned).
-- Used to cap how many partition-days an incremental run ingests.
{% macro max_partition_dt_offset(relation, days) %}
    (
        SELECT date_format(
            date_add('day', {{ days }}, MAX(date(dt))),
            '%Y-%m-%d'
        )
        FROM "{{ relation.schema }}"."{{ relation.identifier }}$partitions"
    )
{% endmacro %}
