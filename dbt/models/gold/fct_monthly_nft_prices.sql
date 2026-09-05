-- Monthly NFT price/volume aggregate per contract. One row per
-- (month, sk_contract); the median is over the UNIT price
-- (usd_total_amount / quantity) so multi-token bundles don't read as
-- one expensive sale.
--
-- On-demand only: the mandatory 'month' var (YYYY-MM) names the single
-- partition each run replaces. tag:manual keeps it out of every
-- automatic run-dbt selection (the Lambda always passes
-- --exclude tag:manual), so it runs only via local dbt:
--   dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-01"}'
-- Backfill = loop over months from 2019-01 (fct_sales floor).
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['month'],
    tags=['manual']
) }}

{# The guard runs only at execution: every dbt invocation renders every
   model at parse time, and raising there would break the pipelines that
   never pass the var. The none default keeps parse-time rendering
   harmless; a real run never sees it because the guard fires first. #}
{% set month = var('month', none) %}
{% if execute and (month is none or not modules.re.match('^\\d{4}-(0[1-9]|1[0-2])$', month)) %}
    {{ exceptions.raise_compiler_error(
        "fct_monthly_nft_prices requires --vars '{\"month\": \"YYYY-MM\"}'; got: "
        ~ var('month', '<missing>')
    ) }}
{% endif %}

SELECT sk_contract,
    COUNT(*) AS sales_count,
    SUM(quantity) AS nft_quantity,
    ROUND(SUM(usd_total_amount), 2) AS usd_total_amount,
    ROUND(approx_percentile(usd_total_amount / quantity, 0.5), 2)
        AS usd_median_unit_price,
    '{{ month }}' AS month
FROM {{ ref('fct_sales') }}
WHERE dt >= '{{ month }}-01'
    AND dt < CAST(date '{{ month }}-01' + INTERVAL '1' MONTH AS varchar)
GROUP BY sk_contract
