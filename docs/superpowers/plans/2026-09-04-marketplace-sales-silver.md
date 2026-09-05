# Silver Marketplace Sales Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `silver.sales` — one deduplicated row per token sold across the five Ethereum marketplaces — as an incremental dbt model, with tests and the `run-dbt` Lambda timeout raised.

**Architecture:** Single dbt model with five CTE branches (one per marketplace, from the user-validated queries) unified by `UNION ALL`; partitioned `(marketplace, dt)` with `insert_overwrite`; bounded incremental advance via `$partitions`; a shared `sales_dt_window` macro applies per-marketplace date bounds to every staging scan.

**Tech Stack:** dbt-athena (Trino SQL), Glue/Athena, Terraform.

**Spec:** `docs/superpowers/specs/2026-09-04-marketplace-sales-silver-design.md`

## Global Constraints

- SQL style: keywords UPPERCASE; explicit `INNER JOIN` (never bare `JOIN`).
- Partition columns last in the SELECT, in `partitioned_by` order: `marketplace, dt`.
- Max/min of a partition column ALWAYS via `"<table>$partitions"` (existing macros), never the data table.
- `total_amount_raw`, `royalty_amount_raw`, `quantity`: `CAST(... AS decimal(38,0))` in every branch (`UNION ALL` needs identical types; staging already stores amounts and quantities as decimal(38,0), token ids as string).
- Dedup by `decoded_at = MAX(decoded_at) OVER (PARTITION BY dt, transaction_hash, log_index)` per staging scan.
- Hardcoded addresses and royalty divisors stay inline with a comment.
- All artifacts in English. Work happens on branch `feat/silver-sales`.
- Verification is against live Athena: `cd dbt && dbt <cmd>` uses the deployed profile. Costs are bounded by partition pruning; never run unbounded scans over the full table.

---

### Task 1: Declare the six new staging sources

**Files:**
- Modify: `dbt/models/silver/sources.yml` (append to the `staging` source block, after `ethereum_nft_transfers`)

**Interfaces:**
- Produces: `source('staging', X)` for X in `ethereum_currency_transfers`, `ethereum_legacy_marketplace_auctions`, `ethereum_marketplace_v2_orders`, `ethereum_wyvern_sales`, `ethereum_seaport_sales`, `ethereum_marketplace_trades` — used by Task 3.

- [ ] **Step 1: Append the six tables to the staging source block**

Column lists match the decode handlers' `_FINAL_COLUMNS` (see `decode/ethereum_*_handler.py`). Append:

```yaml
      - name: ethereum_currency_transfers
        description: >
          ERC-20 Transfer events decoded from bronze.ethereum_logs by the
          decode-ethereum-currency-transfers Lambda. Append-only timestamped
          parquets; downstream dedups by latest decoded_at. Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: token_address
            description: ERC-20 token contract, lowercase.
          - name: from_address
          - name: to_address
          - name: amount_raw
            description: Raw token amount, decimal(38,0).
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt

      - name: ethereum_legacy_marketplace_auctions
        description: >
          LegacyMarketplace auction events (LAND-only marketplace,
          2018-08-30 to 2020-03-07). Append-only; dedup downstream by
          latest decoded_at. Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: event_name
            description: AuctionCreated / AuctionSuccessful / AuctionCancelled.
          - name: auction_id
          - name: asset_id
            description: LAND token id as a decimal string.
          - name: seller
          - name: winner
            description: Auction winner (the buyer), lowercase.
          - name: total_price
            description: MANA raw price, decimal(38,0).
          - name: expires_at
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt

      - name: ethereum_marketplace_v2_orders
        description: >
          Marketplace v2 order events (multi-collection, MANA-only).
          Append-only; dedup downstream by latest decoded_at.
          Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: event_name
            description: OrderCreated / OrderSuccessful / OrderCancelled.
          - name: order_id
          - name: asset_id
            description: Token id as a decimal string.
          - name: contract_address
            description: NFT contract sold, lowercase.
          - name: seller
          - name: buyer
          - name: total_price
            description: MANA raw price, decimal(38,0).
          - name: expires_at
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt

      - name: ethereum_wyvern_sales
        description: >
          OpenSea Wyvern OrdersMatched events (2018-11-06 to 2022-08-01).
          Append-only; dedup downstream by latest decoded_at.
          Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: wyvern_address
          - name: buy_hash
            description: Zero hash when the match came from a listing (maker sells).
          - name: sell_hash
          - name: maker
          - name: taker
          - name: price
            description: Raw price in the payment token, decimal(38,0).
          - name: metadata
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt

      - name: ethereum_seaport_sales
        description: >
          OpenSea Seaport OrderFulfilled events (from 2022-06-03), one row
          per order with NFT and payment legs as parallel arrays.
          Append-only; dedup downstream by latest decoded_at.
          Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: seaport_address
          - name: order_hash
          - name: offerer
          - name: recipient
          - name: order_side
            description: listing or bid.
          - name: buyer
          - name: seller
          - name: nft_contract_addresses
          - name: nft_token_ids
            description: Array of decimal-string token ids, parallel to the other nft_* arrays.
          - name: nft_quantities
            description: Array of decimal(38,0).
          - name: nft_froms
          - name: nft_tos
          - name: payment_currencies
            description: Zero address = native ETH.
          - name: payment_amounts
            description: Array of decimal(38,0).
          - name: payment_recipients
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt

      - name: ethereum_marketplace_trades
        description: >
          Decentraland off-chain marketplace Traded events (from
          2024-12-21), one row per trade with NFT and payment legs as
          parallel arrays. Append-only; dedup downstream by latest
          decoded_at. Partitioned by dt.
        columns:
          - name: transaction_hash
          - name: log_index
          - name: block_timestamp
          - name: marketplace_address
          - name: trade_id
          - name: caller
          - name: signer
          - name: order_side
          - name: buyer
          - name: seller
          - name: nft_contract_addresses
          - name: nft_token_ids
            description: Array of decimal-string token ids.
          - name: nft_asset_types
          - name: nft_beneficiaries
          - name: payment_currencies
          - name: payment_amounts
            description: Array of decimal(38,0).
          - name: payment_asset_types
          - name: payment_beneficiaries
          - name: bronze_extracted_at
          - name: decoded_at
          - name: dt
```

- [ ] **Step 2: Verify dbt parses**

Run: `cd dbt && dbt parse`
Expected: `Done.` with no errors.

- [ ] **Step 3: Commit**

```bash
git add dbt/models/silver/sources.yml
git commit -m "feat: declare marketplace staging sources for silver.sales"
```

---

### Task 2: `sales_dt_window` macro

**Files:**
- Create: `dbt/macros/sales_dt_window.sql`

**Interfaces:**
- Consumes: existing macros `max_partition_dt(relation)`, `max_partition_dt_offset(relation, days)`.
- Produces: `sales_dt_window(start_dt, end_dt=none)` — emits a complete dt predicate (no leading `AND`); callers write `AND {{ sales_dt_window('2018-08-30', '2020-03-07') }}`. Resolves `this` and `is_incremental()` from the calling model's context.

- [ ] **Step 1: Write the macro**

```sql
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
```

- [ ] **Step 2: Verify dbt parses**

Run: `cd dbt && dbt parse`
Expected: `Done.` (full compile verification comes with Task 3, once a model calls it).

- [ ] **Step 3: Commit**

```bash
git add dbt/macros/sales_dt_window.sql
git commit -m "feat: sales_dt_window macro (per-marketplace bounded dt window)"
```

---

### Task 3: `silver.sales` model + initial seed build

**Files:**
- Create: `dbt/models/silver/sales.sql`

**Interfaces:**
- Consumes: `sales_dt_window` (Task 2), the six sources (Task 1), `source('staging', 'ethereum_nft_transfers')`, `ref('dim_contracts')`.
- Produces: table `silver.sales`, grain `(chain_id, transaction_hash, log_index, nft_contract_address, token_id)`, partitioned `(marketplace, dt)` — consumed by Task 4 tests and future gold models.

- [ ] **Step 1: Write the model**

The five branches are the user-validated queries with only the spec's normalizations (windows via macro, `marketplace` instead of `job`, explicit casts, `source()`/`ref()`, dropped no-op `ORDER BY` / unused `contract_name`, lambda params renamed `acc` to avoid shadowing table aliases). Full content:

```sql
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

v2_sales AS (
    SELECT o.transaction_hash,
        o.log_index,
        o.block_timestamp,
        o.buyer AS buyer,
        o.seller AS seller,
        o.contract_address AS nft_contract_address,
        o.asset_id AS token_id,
        CAST(n.quantity AS decimal(38,0)) AS quantity,
        -- MANA
        '0x0f5d2fb29fb7d3cfee444a200298f468908cc942' AS currency,
        CAST(o.total_price AS decimal(38,0)) AS total_amount_raw,
        CAST(COALESCE(m.amount_raw, 0) AS decimal(38,0)) AS royalty_amount_raw,
        o.bronze_extracted_at,
        o.decoded_at,
        'ethereum_marketplace_v2' AS marketplace,
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
    WHERE o.decoded_at = o.latest_decoded_at
        AND n.decoded_at = n.latest_decoded_at
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
    SELECT transaction_hash,
        ARRAY_AGG(DISTINCT contract_address)[1] AS contract_address,
        ARRAY_AGG(token_id) AS token_ids,
        ARRAY_AGG(quantity) AS quantities,
        ARRAY_AGG(DISTINCT from_address)[1] AS from_address,
        ARRAY_AGG(DISTINCT to_address)[1] AS to_address
    FROM wyvern_nft_scans
    WHERE decoded_at = latest_decoded_at
        AND transaction_hash IN (SELECT transaction_hash FROM wyvern_events)
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
```

- [ ] **Step 2: Compile and inspect**

Run: `cd dbt && dbt compile --select sales`
Then read `dbt/target/compiled/decentraland/models/silver/sales.sql` and check: every staging scan carries its `dt >= '<start>'` constant plus `dt < '2019-01-01'` (non-incremental compile), the dead-marketplace branches carry their `dt <= end` bound, and partition columns `marketplace, dt` close the final SELECT.
Expected: compiles clean; predicates present in all ~10 staging scans.

- [ ] **Step 3: Initial seed build (2018 only)**

Run: `cd dbt && dbt run --select sales`
Expected: `OK created ... models` — creates `silver.sales` with dt < 2019-01-01. Legacy auctions, v2 and wyvern partitions only.

- [ ] **Step 4: Validate the seed against the source queries**

Run in Athena (or via `aws athena start-query-execution` on the tagged workgroup):

```sql
SELECT marketplace, COUNT(*) AS rows, MIN(dt) AS first_dt, MAX(dt) AS last_dt
FROM "silver"."sales"
GROUP BY marketplace
ORDER BY marketplace
```

Expected: three marketplaces (`ethereum_legacy_auctions` from 2018-08-30, `ethereum_marketplace_v2` from 2018-10-11, `ethereum_wyvern` from 2018-11-06), all `last_dt <= 2018-12-31`. Counts should match the user's original per-marketplace queries run over the same 2018 windows — if they diverge, stop and reconcile before continuing.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/silver/sales.sql
git commit -m "feat: silver.sales — unified marketplace sales (5 branches)"
```

---

### Task 4: Tests (`schema.yml` + singular)

**Files:**
- Modify: `dbt/models/silver/schema.yml` (append a model entry)
- Create: `dbt/tests/assert_sales_unique_key.sql`
- Create: `dbt/tests/assert_sales_amounts_sane.sql`

**Interfaces:**
- Consumes: `ref('sales')` (Task 3), `max_partition_dt_offset` macro.

- [ ] **Step 1: Append the model entry to `schema.yml`**

Follow the file's existing formatting. Content:

```yaml
  - name: sales
    description: >
      One row per token sold across the five Ethereum marketplaces
      (legacy auctions, marketplace v2, wyvern, seaport, marketplace
      trades). Grain: (chain_id, transaction_hash, log_index,
      nft_contract_address, token_id). Partitioned by (marketplace, dt);
      dedup by latest decoded_at per (dt, transaction_hash, log_index).
    columns:
      - name: transaction_hash
        tests: [not_null]
      - name: log_index
        tests: [not_null]
      - name: block_timestamp
        tests: [not_null]
      - name: buyer
        tests: [not_null]
      - name: seller
        tests: [not_null]
      - name: nft_contract_address
        tests: [not_null]
      - name: token_id
        tests: [not_null]
      - name: quantity
        tests: [not_null]
      - name: currency
        description: Payment token address; zero address = native ETH.
        tests: [not_null]
      - name: total_amount_raw
        tests: [not_null]
      - name: royalty_amount_raw
        description: >
          Raw royalty amount. NULL only on seaport sales without an
          identified seller.
      - name: bronze_extracted_at
      - name: decoded_at
      - name: silver_processed_at
      - name: chain_id
        tests:
          - not_null
          - accepted_values:
              values: [1]
              quote: false
      - name: marketplace
        tests:
          - not_null
          - accepted_values:
              values:
                - ethereum_legacy_auctions
                - ethereum_marketplace_v2
                - ethereum_wyvern
                - ethereum_seaport
                - ethereum_marketplace_trades
      - name: dt
        tests: [not_null]
```

- [ ] **Step 2: Write `assert_sales_unique_key.sql`**

```sql
-- The declared grain must be unique. Bounded to the window the current
-- run just loaded (max dt minus the incremental cap): an unbounded scan
-- of the full table would blow the workgroup cap; older partitions were
-- validated when loaded (nft_transfers lesson).
SELECT chain_id,
    transaction_hash,
    log_index,
    nft_contract_address,
    token_id
FROM {{ ref('sales') }}
WHERE dt > {{ max_partition_dt_offset(ref('sales'), -1 * var('sales_incremental_days', 10)) }}
GROUP BY chain_id,
    transaction_hash,
    log_index,
    nft_contract_address,
    token_id
HAVING COUNT(*) > 1
```

- [ ] **Step 3: Write `assert_sales_amounts_sane.sql`**

```sql
-- Amount sanity over the freshly loaded window; also the canary for
-- float64-style price corruption (see the 2026-09-04 marketplace v2
-- incident): corrupted prices tend to break the royalty/total relation.
SELECT marketplace,
    transaction_hash,
    log_index,
    total_amount_raw,
    royalty_amount_raw
FROM {{ ref('sales') }}
WHERE dt > {{ max_partition_dt_offset(ref('sales'), -1 * var('sales_incremental_days', 10)) }}
    AND (total_amount_raw < 0
        OR royalty_amount_raw < 0
        OR royalty_amount_raw > total_amount_raw)
```

- [ ] **Step 4: Run the tests**

Run: `cd dbt && dbt test --select sales`
Expected: all PASS against the seeded 2018 data. Note the singular tests' window is bounded relative to `max(dt)` so they cover the seed. If a not_null or uniqueness test fails, stop: reconcile against staging before any further loading (do NOT relax the test to pass).

- [ ] **Step 5: Commit**

```bash
git add dbt/models/silver/schema.yml dbt/tests/assert_sales_unique_key.sql dbt/tests/assert_sales_amounts_sane.sql
git commit -m "test: schema and singular tests for silver.sales"
```

---

### Task 5: Raise `run-dbt` Lambda timeout to 900 s

**Files:**
- Modify: `terraform/lambda_run_dbt.tf:154` (`timeout = 300` → `timeout = 900`)

**Interfaces:**
- None (infra only; memory_size stays 1024 — dbt only orchestrates, Athena does the heavy work).

- [ ] **Step 1: Edit the timeout**

```hcl
  timeout     = 900
  memory_size = 1024
```

- [ ] **Step 2: Plan and review**

Run: `cd terraform && terraform plan`
Expected: exactly one in-place change (`aws_lambda_function.run_dbt` timeout 300 → 900). ALWAYS read the plan before applying.

- [ ] **Step 3: Apply**

Run: `cd terraform && terraform apply`
Expected: `Apply complete! Resources: 0 added, 1 changed, 0 destroyed.`

- [ ] **Step 4: Commit**

```bash
git add terraform/lambda_run_dbt.tf
git commit -m "feat: raise run-dbt timeout to 900s for silver.sales builds"
```

---

### Task 6: Incremental catch-up backfill (2019 → today)

**Files:**
- None created; operational task, mirrors the nft_transfers catch-up recipe.

**Interfaces:**
- Consumes: the deployed model and tests (Tasks 3-4).

- [ ] **Step 1: Advance in 180-day windows**

Run repeatedly until `max(dt)` reaches ~today (≈16 runs from 2019-01-01):

```bash
cd dbt && dbt build --select sales --vars '{sales_incremental_days: 180}'
```

`dbt build` runs the model AND its tests each pass, so every window is validated as it loads. Expected per run: model OK + tests PASS. Watch for the known empty-window stall (a window with zero sales writes no partition and `max(dt)` stops advancing — plausible in thin 2019-2021 stretches): if two consecutive runs report the same `max(dt)`, bump the var (e.g. 365) for one run and drop it back.

Check progress between runs (zero-scan):

```sql
SELECT MAX(dt) FROM "silver"."sales$partitions"
```

- [ ] **Step 2: Final validation**

Run in Athena:

```sql
SELECT marketplace, COUNT(*) AS rows, MIN(dt) AS first_dt, MAX(dt) AS last_dt
FROM "silver"."sales"
GROUP BY marketplace
ORDER BY marketplace
```

Expected: five marketplaces; legacy ends ≤ 2020-03-07, wyvern ends ≤ 2022-08-01, seaport starts 2022-06-03, trades starts 2024-12-21, v2 spans 2018-10-11 → recent. Spot-check totals per marketplace against the user's original queries on a couple of sample windows.

- [ ] **Step 3: Commit nothing — push and PR**

No file changes in this task. With the user's explicit confirmation (per workflow), push the branch and open the PR:

```bash
git push -u origin feat/silver-sales
gh pr create --title "feat: silver.sales — unified marketplace sales" --body "$(cat <<'EOF'
Builds silver.sales: one row per token sold across the five Ethereum
marketplaces (legacy auctions, marketplace v2, wyvern, seaport,
marketplace trades), incremental insert_overwrite partitioned by
(marketplace, dt), deduped by latest decoded_at, with bounded schema
and singular tests. Raises the run-dbt Lambda timeout to 900s.

Spec: docs/superpowers/specs/2026-09-04-marketplace-sales-silver-design.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Squash merge per repo convention.
