-- Shared dt window for every staging scan in silver.sales.
-- start_dt: first day the marketplace traded (constant, prunes scans
-- before it existed). end_dt: last day of a dead marketplace — a scan
-- cap, not a semantic filter; once the incremental front passes it the
-- branch compiles to an empty zero-cost scan. Initial build seeds
-- dt < '2019-01-01'; incremental runs advance at most
-- sales_incremental_days (default 10) past the table's max dt,
-- both bounds zero-scan via "$partitions".
{% macro sales_dt_window(start_dt, end_dt=none) %}
    dt >= '{{ start_dt }}'
    {% if end_dt %}
    AND dt <= '{{ end_dt }}'
    {% endif %}
    {% if is_incremental() %}
    AND dt > {{ max_partition_dt(this) }}
    AND dt <= {{ max_partition_dt_offset(this, var('sales_incremental_days', 10)) }}
    {% else %}
    AND dt < '2019-01-01'
    {% endif %}
{% endmacro %}
