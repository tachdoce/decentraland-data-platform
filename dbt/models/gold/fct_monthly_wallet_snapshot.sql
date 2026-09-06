-- Monthly wallet-state snapshot. Grain: one row per (month,
-- wallet_address) for every wallet that received a DCL NFT or MANA at
-- least once before the month started. month = 'YYYY-MM' means the
-- state at the START of that month (cutoff dt < '<month>-01');
-- valuations use fct_monthly_nft_prices of the previous month, the
-- last closed one at that point.
--
-- ERC-721 ownership resolves the latest transfer per (sk_contract,
-- token_id) by (block_timestamp, log_index) — log_index breaks
-- same-block ties. ERC-1155 is a net quantity sum. mana_balance is
-- NULL (not 0) below the 0.001 dust threshold, distinguishable from
-- an exact 0 holding. The zero address is excluded once, in the final
-- SELECT.
--
-- On-demand only: the mandatory 'month' var (YYYY-MM) names the single
-- partition each run replaces. tag:manual keeps it out of every
-- automatic run-dbt selection (the Lambda always passes
-- --exclude tag:manual), so it runs only via local dbt:
--   dbt build --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'
-- Backfill = loop over months from 2020-01.
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
        "fct_monthly_wallet_snapshot requires --vars '{\"month\": \"YYYY-MM\"}'; got: "
        ~ var('month', '<missing>')
    ) }}
{% endif %}

WITH nft_first_time AS (
    SELECT t.to_address AS wallet_address,
        MIN(CASE WHEN c.contract_name = 'LANDProxy'
            THEN t.block_timestamp END) AS land_owner_first_time,
        MIN(CASE WHEN c.contract_name = 'DCLRegistrar'
            THEN t.block_timestamp END) AS dclregistrar_first_time,
        MIN(CASE WHEN c.contract_name = 'DCLLaunchCollection'
            THEN t.block_timestamp END) AS dcllaunchcollection_first_time,
        MIN(CASE WHEN c.contract_name NOT IN
                ('LANDProxy', 'DCLRegistrar', 'DCLLaunchCollection')
            THEN t.block_timestamp END) AS dcl_others_first_time
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
    WHERE c.dcl_contract
        AND t.dt < '{{ month }}-01'
    GROUP BY t.to_address
),

mana_first_time AS (
    SELECT to_address AS wallet_address,
        MIN(block_timestamp) AS mana_first_time
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
    GROUP BY to_address
),

first_time AS (
    SELECT COALESCE(n.wallet_address, m.wallet_address) AS wallet_address,
        n.land_owner_first_time,
        n.dclregistrar_first_time,
        n.dcllaunchcollection_first_time,
        n.dcl_others_first_time,
        m.mana_first_time
    FROM nft_first_time AS n
        FULL OUTER JOIN mana_first_time AS m
            ON n.wallet_address = m.wallet_address
),

mana_movements AS (
    SELECT to_address AS wallet_address,
        amount
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
    UNION ALL
    SELECT from_address AS wallet_address,
        -amount AS amount
    FROM {{ ref('fct_mana_transfers') }}
    WHERE dt < '{{ month }}-01'
),

mana_balance AS (
    SELECT wallet_address,
        ROUND(SUM(amount), 2) AS mana_balance
    FROM mana_movements
    GROUP BY wallet_address
    HAVING SUM(amount) > 0.001
),

erc721_latest AS (
    SELECT t.sk_contract,
        t.token_id,
        t.to_address AS wallet_address,
        ROW_NUMBER() OVER (
            PARTITION BY t.sk_contract, t.token_id
            ORDER BY t.block_timestamp DESC, t.log_index DESC
        ) AS rn
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 721
    WHERE t.dt < '{{ month }}-01'
),

erc721_holdings AS (
    SELECT wallet_address,
        sk_contract,
        COUNT(*) AS quantity
    FROM erc721_latest
    WHERE rn = 1
    GROUP BY wallet_address, sk_contract
),

erc1155_movements AS (
    SELECT t.to_address AS wallet_address,
        t.sk_contract,
        t.quantity
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 1155
    WHERE t.dt < '{{ month }}-01'
    UNION ALL
    SELECT t.from_address AS wallet_address,
        t.sk_contract,
        -t.quantity AS quantity
    FROM {{ ref('fct_nft_transfers') }} AS t
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON t.sk_contract = c.sk_contract
            AND c.erc_type = 1155
    WHERE t.dt < '{{ month }}-01'
),

erc1155_holdings AS (
    SELECT wallet_address,
        sk_contract,
        SUM(quantity) AS quantity
    FROM erc1155_movements
    GROUP BY wallet_address, sk_contract
    HAVING SUM(quantity) > 0
),

holdings AS (
    SELECT wallet_address, sk_contract, quantity FROM erc721_holdings
    UNION ALL
    SELECT wallet_address, sk_contract, quantity FROM erc1155_holdings
),

holdings_value AS (
    SELECT h.wallet_address,
        SUM(CASE WHEN c.dcl_contract THEN 1 ELSE 0 END) >= 1
            AS own_dcl_nfts,
        SUM(CASE WHEN NOT c.dcl_contract THEN 1 ELSE 0 END) >= 1
            AS own_other_nfts,
        ROUND(SUM(CASE WHEN c.dcl_contract
            THEN COALESCE(p.usd_median_unit_price * h.quantity, 0)
            ELSE 0 END), 2) AS dcl_nft_value,
        ROUND(SUM(CASE WHEN NOT c.dcl_contract
            THEN COALESCE(p.usd_median_unit_price * h.quantity, 0)
            ELSE 0 END), 2) AS other_nft_value
    FROM holdings AS h
        INNER JOIN {{ ref('dim_nft_contracts') }} AS c
            ON h.sk_contract = c.sk_contract
        LEFT JOIN {{ ref('fct_monthly_nft_prices') }} AS p
            ON h.sk_contract = p.sk_contract
            AND p.month = DATE_FORMAT(
                DATE '{{ month }}-01' - INTERVAL '1' MONTH, '%Y-%m')
    GROUP BY h.wallet_address
)

SELECT f.wallet_address,
    f.land_owner_first_time,
    f.dclregistrar_first_time,
    f.dcllaunchcollection_first_time,
    f.dcl_others_first_time,
    f.mana_first_time,
    b.mana_balance,
    COALESCE(h.own_dcl_nfts, FALSE) AS own_dcl_nfts,
    COALESCE(h.own_other_nfts, FALSE) AS own_other_nfts,
    COALESCE(h.dcl_nft_value, 0) AS dcl_nft_value,
    COALESCE(h.other_nft_value, 0) AS other_nft_value,
    CAST(current_timestamp AS timestamp) AS gold_processed_at,
    '{{ month }}' AS month
FROM first_time AS f
    LEFT JOIN mana_balance AS b
        ON f.wallet_address = b.wallet_address
    LEFT JOIN holdings_value AS h
        ON f.wallet_address = h.wallet_address
WHERE f.wallet_address <> '0x0000000000000000000000000000000000000000'
