-- One row per token sold across the five relevant Ethereum marketplaces.
-- Branch logic comes from the per-marketplace queries validated in the
-- 2026-09-04 spec; every staging scan dedups by latest decoded_at per
-- (dt, transaction_hash, log_index) and applies sales_dt_window for
-- partition pruning (marketplace start/end constants + seed/incremental
-- bounds).
--
-- The initial build seeds dt < '2019-01-01' (legacy auctions,
-- marketplace v2, wyvern); seaport (2022-06-03) and marketplace trades
-- (2024-12-21) arrive through capped incremental runs, e.g.
-- --vars '{sales_incremental_days: 180}' to catch up.
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['marketplace', 'dt']
) }}

WITH

-- ============================================================
-- ethereum_legacy_auctions: LAND-only marketplace, MANA-only.
-- Dead 2020-03-07.
-- ============================================================
legacy_lands AS (
    SELECT transaction_hash,
        token_id,
        decoded_at,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_nft_transfers') }}
    -- LAND
    WHERE contract_address = '0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d'
        AND {{ sales_dt_window('2018-08-30', '2020-03-07') }}
),

legacy_auctions AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_legacy_marketplace_auctions') }}
    WHERE event_name = 'AuctionSuccessful'
        AND {{ sales_dt_window('2018-08-30', '2020-03-07') }}
),

legacy_sales AS (
    SELECT a.transaction_hash,
        a.log_index,
        a.block_timestamp,
        a.winner AS buyer,
        a.seller AS seller,
        -- LAND
        '0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d' AS nft_contract_address,
        a.asset_id AS token_id,
        CAST(1 AS decimal(38,0)) AS quantity,
        -- MANA
        '0x0f5d2fb29fb7d3cfee444a200298f468908cc942' AS currency,
        CAST(a.total_price AS decimal(38,0)) AS total_amount_raw,
        CAST(0 AS decimal(38,0)) AS royalty_amount_raw,
        a.bronze_extracted_at,
        a.decoded_at,
        'ethereum_legacy_auctions' AS marketplace,
        a.dt
    FROM legacy_auctions AS a
        INNER JOIN legacy_lands AS l
            ON a.transaction_hash = l.transaction_hash
            AND a.asset_id = l.token_id
    WHERE a.decoded_at = a.latest_decoded_at
        AND l.decoded_at = l.latest_decoded_at
),

-- ============================================================
-- ethereum_marketplace_v2: multi-collection, MANA-only. Royalty =
-- MANA transfer from the buyer to the fee/royalty wallets.
-- ============================================================
v2_nfts AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_nft_transfers') }}
    WHERE {{ sales_dt_window('2018-10-11') }}
),

v2_orders AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_marketplace_v2_orders') }}
    WHERE event_name = 'OrderSuccessful'
        AND {{ sales_dt_window('2018-10-11') }}
),

v2_mana AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_currency_transfers') }}
    -- MANA
    WHERE token_address = '0x0f5d2fb29fb7d3cfee444a200298f468908cc942'
        AND to_address IN (
            -- marketplace fee collector
            '0xadfeb1de7876fcabeaf87df5a6c566b70f970018',
            -- DCL DAO royalties wallet
            '0x9a6ebe7e2a7722f8200d0ffb63a1f6406a0d7dce'
        )
        AND {{ sales_dt_window('2018-10-11') }}
),

v2_joined AS (
    -- A multi-sale tx carries one fee/royalty MANA transfer per order,
    -- each emitted right before its OrderSuccessful; joining on
    -- (tx, buyer) alone fans out every order against every transfer.
    -- Keep only the closest transfer preceding each order's log.
    SELECT o.transaction_hash,
        o.log_index,
        o.block_timestamp,
        o.buyer,
        o.seller,
        o.contract_address,
        o.asset_id,
        n.quantity,
        o.total_price,
        m.amount_raw,
        m.log_index AS mana_log_index,
        MAX(m.log_index) OVER (
            PARTITION BY o.dt, o.transaction_hash, o.log_index
        ) AS closest_mana_log_index,
        o.bronze_extracted_at,
        o.decoded_at,
        o.dt
    FROM v2_orders AS o
        INNER JOIN v2_nfts AS n
            ON o.transaction_hash = n.transaction_hash
            AND o.contract_address = n.contract_address
            AND o.asset_id = n.token_id
            AND o.seller = n.from_address
            AND o.buyer = n.to_address
        LEFT JOIN v2_mana AS m
            ON o.transaction_hash = m.transaction_hash
            AND m.decoded_at = m.latest_decoded_at
            AND o.buyer = m.from_address
            AND m.log_index < o.log_index
    WHERE o.decoded_at = o.latest_decoded_at
        AND n.decoded_at = n.latest_decoded_at
),

v2_sales AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        buyer,
        seller,
        contract_address AS nft_contract_address,
        asset_id AS token_id,
        CAST(quantity AS decimal(38,0)) AS quantity,
        -- MANA
        '0x0f5d2fb29fb7d3cfee444a200298f468908cc942' AS currency,
        CAST(total_price AS decimal(38,0)) AS total_amount_raw,
        CAST(COALESCE(amount_raw, 0) AS decimal(38,0)) AS royalty_amount_raw,
        bronze_extracted_at,
        decoded_at,
        'ethereum_marketplace_v2' AS marketplace,
        dt
    FROM v2_joined
    WHERE mana_log_index IS NULL
        OR mana_log_index = closest_mana_log_index
),

-- ============================================================
-- ethereum_wyvern: OpenSea Wyvern, single-collection txs only
-- (HAVING guards), price prorated across transferred tokens.
-- Dead 2022-08-01.
-- ============================================================
wyvern_events AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        CASE buy_hash
            WHEN '0x0000000000000000000000000000000000000000000000000000000000000000'
            THEN maker ELSE taker
        END AS from_address,
        CASE buy_hash
            WHEN '0x0000000000000000000000000000000000000000000000000000000000000000'
            THEN taker ELSE maker
        END AS to_address,
        price,
        bronze_extracted_at,
        decoded_at,
        dt,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_wyvern_sales') }}
    WHERE {{ sales_dt_window('2018-11-06', '2022-08-01') }}
),

wyvern_nft_scans AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_nft_transfers') }}
    WHERE {{ sales_dt_window('2018-11-06', '2022-08-01') }}
),

wyvern_nfts AS (
    -- The inner GROUP BY collapses multi-leg transfers of the SAME
    -- token in one tx (an 1155 can move as several Transfer logs, e.g.
    -- qty 554 + qty 1) into one position summing quantity; without it
    -- the UNNEST downstream would emit duplicate grain rows.
    SELECT transaction_hash,
        ARRAY_AGG(DISTINCT contract_address)[1] AS contract_address,
        ARRAY_AGG(token_id) AS token_ids,
        ARRAY_AGG(quantity) AS quantities,
        ARRAY_AGG(DISTINCT from_address)[1] AS from_address,
        ARRAY_AGG(DISTINCT to_address)[1] AS to_address
    FROM (
        SELECT transaction_hash,
            contract_address,
            token_id,
            SUM(quantity) AS quantity,
            from_address,
            to_address
        FROM wyvern_nft_scans
        WHERE decoded_at = latest_decoded_at
            AND transaction_hash IN (SELECT transaction_hash FROM wyvern_events)
        GROUP BY transaction_hash,
            contract_address,
            token_id,
            from_address,
            to_address
    )
    GROUP BY transaction_hash
    HAVING CARDINALITY(ARRAY_AGG(DISTINCT contract_address)) = 1
        AND CARDINALITY(ARRAY_AGG(DISTINCT from_address)) = 1
        AND CARDINALITY(ARRAY_AGG(DISTINCT to_address)) = 1
),

wyvern_currency_scans AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_currency_transfers') }}
    WHERE {{ sales_dt_window('2018-11-06', '2022-08-01') }}
),

wyvern_currencies AS (
    SELECT transaction_hash,
        ARRAY_AGG(token_address)[1] AS currency,
        ARRAY_AGG(from_address)[1] AS from_address,
        ARRAY_AGG(to_address)[1] AS to_address,
        ARRAY_AGG(amount_raw)[1] AS amount_raw
    FROM wyvern_currency_scans
    WHERE decoded_at = latest_decoded_at
        AND transaction_hash IN (SELECT transaction_hash FROM wyvern_events)
        -- OpenSea Wallet 2 (fees)
        AND to_address <> '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
    GROUP BY transaction_hash
    HAVING COUNT(*) = 1
),

wyvern_full AS (
    SELECT s.*,
        n.contract_address,
        n.token_ids,
        n.quantities,
        -- zero address = native ETH
        COALESCE(c.currency, '0x0000000000000000000000000000000000000000') AS currency
    FROM wyvern_events AS s
        INNER JOIN wyvern_nfts AS n
            ON s.transaction_hash = n.transaction_hash
            AND s.to_address = n.to_address
        LEFT JOIN wyvern_currencies AS c
            ON s.transaction_hash = c.transaction_hash
    WHERE s.decoded_at = s.latest_decoded_at
        AND ((c.transaction_hash IS NULL)
            OR (c.transaction_hash IS NOT NULL
                AND s.price = c.amount_raw
                AND s.to_address = c.from_address))
),

wyvern_sales AS (
    SELECT s.transaction_hash,
        s.log_index,
        s.block_timestamp,
        s.to_address AS buyer,
        s.from_address AS seller,
        s.contract_address AS nft_contract_address,
        n.token_id,
        CAST(n.quantity AS decimal(38,0)) AS quantity,
        s.currency,
        CAST(s.price * n.quantity / REDUCE(
            s.quantities, CAST(0 AS decimal(38,0)),
            (acc, x) -> acc + x, acc -> acc
        ) AS decimal(38,0)) AS total_amount_raw,
        -- / 20 = 5% DCL royalty
        CAST(CASE d.dcl_contract
            WHEN TRUE THEN s.price * n.quantity / REDUCE(
                s.quantities, CAST(0 AS decimal(38,0)),
                (acc, x) -> acc + x, acc -> acc
            ) / 20
            ELSE 0
        END AS decimal(38,0)) AS royalty_amount_raw,
        s.bronze_extracted_at,
        s.decoded_at,
        'ethereum_wyvern' AS marketplace,
        s.dt
    FROM wyvern_full AS s
        CROSS JOIN UNNEST(s.token_ids, s.quantities) AS n (token_id, quantity)
        INNER JOIN {{ ref('dim_contracts') }} AS d
            ON d.chain_id = 1
            AND d.contract_address = s.contract_address
),

-- ============================================================
-- ethereum_seaport: OpenSea Seaport, arrays per order; payments
-- attributed via recipients; bid+listing pairs collapse to the
-- bid row (self-join guards). Single-collection, single-currency
-- orders only.
-- ============================================================
seaport_ranked AS (
    SELECT s.*,
        MAX(s.decoded_at) OVER (
            PARTITION BY s.dt, s.transaction_hash, s.log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_seaport_sales') }} AS s
    WHERE {{ sales_dt_window('2022-06-03') }}
),

seaport_orders AS (
    SELECT *
    FROM seaport_ranked
    WHERE decoded_at = latest_decoded_at
),

opensea_fee_wallets AS (
    SELECT wallet FROM (
        VALUES
            ('0x0000a26b00c1f0df003000390027140000faa719'),  -- OpenSea: Fees 3 (Seaport)
            ('0x8de9c5a032463c561423387a9648c5c7bcc5bc90'),  -- OpenSea Wallet (legacy)
            ('0x5b3256965e7c3cf26e11fcaf296dfc8807c01073')   -- OpenSea Wallet 2
    ) AS t (wallet)
),

seaport_payments AS (
    SELECT o.transaction_hash,
        o.log_index,
        o.dt,
        COUNT(DISTINCT p.currency) AS currency_count,
        MIN(p.currency) AS currency,
        SUM(p.amount) AS total_amount_raw,
        SUM(CASE WHEN p.recipient = o.seller THEN p.amount END) AS seller_amount_raw,
        SUM(CASE WHEN f.wallet IS NOT NULL THEN p.amount END) AS fee_amount_raw,
        -- DCL DAO royalties wallet
        SUM(CASE WHEN p.recipient = '0x9a6ebe7e2a7722f8200d0ffb63a1f6406a0d7dce'
            THEN p.amount END) AS royalty_amount_raw
    FROM seaport_orders AS o
        CROSS JOIN UNNEST(
            o.payment_currencies, o.payment_amounts, o.payment_recipients
        ) AS p (currency, amount, recipient)
        LEFT JOIN opensea_fee_wallets AS f
            ON p.recipient = f.wallet
    GROUP BY o.transaction_hash, o.log_index, o.dt
),

seaport_nft_items AS (
    SELECT o.transaction_hash,
        o.log_index,
        o.block_timestamp,
        o.order_side,
        o.buyer,
        o.seller,
        n.nft_contract_address,
        n.token_id,
        n.quantity,
        REDUCE(
            o.nft_quantities, CAST(0 AS decimal(38,0)),
            (acc, x) -> acc + x, acc -> acc
        ) AS nft_count,
        o.bronze_extracted_at,
        o.decoded_at,
        o.dt
    FROM seaport_orders AS o
        CROSS JOIN UNNEST(
            o.nft_contract_addresses, o.nft_token_ids, o.nft_quantities,
            o.nft_froms, o.nft_tos
        ) AS n (nft_contract_address, token_id, quantity, nft_from, nft_to)
    WHERE CARDINALITY(ARRAY_DISTINCT(o.nft_contract_addresses)) = 1
),

seaport_base AS (
    SELECT i.transaction_hash,
        i.log_index,
        i.block_timestamp,
        i.order_side,
        i.buyer,
        i.seller,
        i.nft_contract_address,
        i.token_id,
        i.quantity,
        i.nft_count,
        p.currency AS currency,
        p.total_amount_raw AS total_amount_raw,
        CASE WHEN i.seller IS NOT NULL THEN p.seller_amount_raw END AS seller_amount_raw,
        p.fee_amount_raw AS fee_amount_raw,
        CASE WHEN i.seller IS NOT NULL THEN p.royalty_amount_raw END AS royalty_amount_raw,
        i.bronze_extracted_at,
        i.decoded_at,
        i.dt
    FROM seaport_nft_items AS i
        INNER JOIN seaport_payments AS p
            ON i.transaction_hash = p.transaction_hash
            AND i.log_index = p.log_index
            AND i.dt = p.dt
        INNER JOIN {{ ref('dim_contracts') }} AS d
            ON d.chain_id = 1
            AND d.contract_address = i.nft_contract_address
    WHERE p.currency_count = 1
),

seaport_sales AS (
    SELECT sc.transaction_hash,
        sc.log_index,
        sc.block_timestamp,
        sc.buyer,
        sc.seller,
        sc.nft_contract_address,
        sc.token_id,
        CAST(sc.quantity AS decimal(38,0)) AS quantity,
        sc.currency,
        CAST(
            CASE WHEN sa.transaction_hash IS NOT NULL
                THEN sc.seller_amount_raw ELSE sc.total_amount_raw
            END * sc.quantity / sc.nft_count
        AS decimal(38,0)) AS total_amount_raw,
        CAST(
            CASE WHEN sa.transaction_hash IS NOT NULL
                THEN sa.royalty_amount_raw ELSE sc.royalty_amount_raw
            END * sc.quantity / sc.nft_count
        AS decimal(38,0)) AS royalty_amount_raw,
        sc.bronze_extracted_at,
        sc.decoded_at,
        'ethereum_seaport' AS marketplace,
        sc.dt
    FROM seaport_base AS sc
        LEFT JOIN seaport_base AS sa
            ON sc.transaction_hash = sa.transaction_hash
            AND sc.log_index + 1 = sa.log_index
            AND sc.order_side = 'bid'
            AND sa.order_side = 'listing'
        LEFT JOIN seaport_base AS sb
            ON sc.transaction_hash = sb.transaction_hash
            AND sc.log_index = sb.log_index + 1
            AND sc.order_side = 'listing'
            AND sb.order_side = 'bid'
            AND sc.buyer = sc.seller
    WHERE sb.transaction_hash IS NULL
),

-- ============================================================
-- ethereum_marketplace_trades: DCL off-chain marketplace Traded
-- events (from 2024-12-21), one NFT per payment leg.
-- ============================================================
trades_exploded AS (
    SELECT *,
        MAX(decoded_at) OVER (
            PARTITION BY dt, transaction_hash, log_index
        ) AS latest_decoded_at
    FROM {{ source('staging', 'ethereum_marketplace_trades') }}
        CROSS JOIN UNNEST(nft_contract_addresses, nft_token_ids)
            AS p (nft_contract_address, nft_token_id)
    WHERE {{ sales_dt_window('2024-12-21') }}
),

trades_sales AS (
    SELECT transaction_hash,
        log_index,
        block_timestamp,
        buyer AS buyer,
        seller AS seller,
        nft_contract_address,
        nft_token_id AS token_id,
        CAST(1 AS decimal(38,0)) AS quantity,
        payment_currencies[1] AS currency,
        CAST(payment_amounts[1] AS decimal(38,0)) AS total_amount_raw,
        -- / 40 = 2.5% royalty
        CAST(payment_amounts[1] / 40 AS decimal(38,0)) AS royalty_amount_raw,
        bronze_extracted_at,
        decoded_at,
        'ethereum_marketplace_trades' AS marketplace,
        dt
    FROM trades_exploded
    WHERE decoded_at = latest_decoded_at
),

unioned AS (
    SELECT * FROM legacy_sales
    UNION ALL
    SELECT * FROM v2_sales
    UNION ALL
    SELECT * FROM wyvern_sales
    UNION ALL
    SELECT * FROM seaport_sales
    UNION ALL
    SELECT * FROM trades_sales
)

SELECT transaction_hash,
    log_index,
    block_timestamp,
    buyer,
    seller,
    nft_contract_address,
    token_id,
    quantity,
    currency,
    total_amount_raw,
    royalty_amount_raw,
    bronze_extracted_at,
    decoded_at,
    CAST(current_timestamp AS timestamp) AS silver_processed_at,
    1 AS chain_id,
    marketplace,
    dt
FROM unioned
