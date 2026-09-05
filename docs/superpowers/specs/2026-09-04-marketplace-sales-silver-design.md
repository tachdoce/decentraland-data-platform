# Silver marketplace sales — design

Date: 2026-09-04
Status: validated with the user (sections discussed and approved in chat)

## Goal

Unify the five decoded marketplace staging tables into `silver.sales`:
one clean, deduplicated, incrementally built fact table of NFT sales on
Ethereum, ready for gold aggregation (volumes, royalties, price joins).
The five user-authored queries (one per marketplace) are the source of
truth for the per-marketplace logic; this model integrates them with
shared windowing, dedup and typing.

Sources (all `staging` schema):

| marketplace value | staging table | active window |
|---|---|---|
| `ethereum_legacy_auctions` | `ethereum_legacy_marketplace_auctions` | 2018-08-30 → 2020-03-07 (dead) |
| `ethereum_marketplace_v2` | `ethereum_marketplace_v2_orders` | 2018-10-11 → open |
| `ethereum_wyvern` | `ethereum_wyvern_sales` | 2018-11-06 → 2022-08-01 (dead) |
| `ethereum_seaport` | `ethereum_seaport_sales` | 2022-06-03 → open |
| `ethereum_marketplace_trades` | `ethereum_marketplace_trades` | 2024-12-21 → open |

Helper scans: `staging.ethereum_nft_transfers` (legacy, v2, wyvern),
`staging.ethereum_currency_transfers` (v2, wyvern), and
`silver.dim_contracts` via `ref()` (wyvern, seaport).

## Key decisions

1. **Single model, five branches.** One `dbt/models/silver/sales.sql`
   with a CTE group per marketplace and a final `UNION ALL`. One
   incremental table, one advance front, one place to test. The
   per-marketplace queries stay readable as independent CTE groups.
2. **Grain**: one row per token sold. Unique key
   `(chain_id, transaction_hash, log_index, nft_contract_address,
   token_id)` — wyvern, seaport and marketplace trades explode multiple
   NFTs per sale event.
3. **Double partition `(marketplace, dt)`** (user decision), not
   `(chain_id, dt)`: allows reprocessing one marketplace's partitions
   with `insert_overwrite` without touching the others. `chain_id`
   remains a regular column (constant 1 for now); the `ethereum_`
   prefix in the marketplace value keeps S3 paths self-describing.
4. **Dedup by latest `decoded_at` AND latest `bronze_extracted_at`**
   per `(dt, transaction_hash, log_index)`, applied per staging scan.
   Amended during the backfill: `decoded_at` alone (the original user
   decision) cannot separate several bronze copies of the same log
   ingested by ONE decode run — a retry storm on 2026-08-24 left 10
   copies per log, all sharing one `decoded_at`. Follow-up: keep
   `silver.nft_transfers` on `bronze_extracted_at` (its criterion was
   right); consider adding the dual criterion there too.
5. **Bounded incremental, `nft_transfers` pattern.** First build seeds
   `dt < '2019-01-01'` (covers the three 2018 marketplaces; seaport and
   trades arrive via catch-up). Incremental runs load
   `(max(dt), max(dt) + N]` with
   `N = var('sales_incremental_days', 10)`, both bounds zero-scan via
   `$partitions`. The advance front is global across marketplaces.
6. **Shared window macro** `sales_dt_window(start_dt, end_dt=none)`
   emits the full dt predicate (marketplace start constant, optional
   dead-marketplace end constant, seed cutoff / capped incremental).
   Applied inside EVERY staging CTE — including helper CTEs — so
   partition pruning reaches each scan. Joins are intra-transaction
   (same dt), so shared windows lose no rows at the edges.
7. **Dead-marketplace end bounds** (`2020-03-07` legacy auctions,
   `2022-08-01` wyvern): once the incremental front passes the end
   date, those branches compile to empty zero-cost scans — daily runs
   stop touching their staging tables. The bound is a scan cap, not a
   semantic filter; only set where the contract is known dead, with a
   comment.
8. **`marketplace` column replaces `job`** ('01'..'05'): readable
   values listed in the table above (user decision, `ethereum_`
   prefix included).
9. **Uniform typing across branches**: `total_amount_raw` and
   `royalty_amount_raw` explicitly `CAST(... AS decimal(38,0))` in all
   five branches — `UNION ALL` requires identical types.
10. **Hardcoded constants stay hardcoded, commented**: LAND and MANA
    addresses, marketplace fee/royalty wallets, OpenSea fee wallets,
    and the royalty divisors (`/ 20` = 5% DCL royalty on wyvern,
    `/ 40` = 2.5% on marketplace trades). They are contract
    properties, not configuration.
11. **Lineage columns** as in `nft_transfers`: `bronze_extracted_at`
    and `decoded_at` pass through; `silver_processed_at =
    CAST(current_timestamp AS timestamp)`.
12. **`run-dbt` Lambda**: memory stays at 1024 MB (dbt only
    orchestrates; Athena does the heavy work) but `timeout` rises
    300 → 900 s in the same PR — the initial seed plus tests can
    approach 300 s.

## Schema

Identical across the five branches; partition columns last, in
`partitioned_by` order:

`transaction_hash`, `log_index`, `block_timestamp`, `buyer`, `seller`,
`nft_contract_address`, `token_id`, `quantity`, `currency` (payment
token address; zero address = native ETH), `total_amount_raw`
decimal(38,0), `royalty_amount_raw` decimal(38,0) (nullable only in
seaport when no seller is identified), `bronze_extracted_at`,
`decoded_at`, `silver_processed_at`, `chain_id`, `marketplace`, `dt`.

## Per-branch normalizations vs the source queries

Logic is preserved; only these adjustments:

- **All branches**: hardcoded date windows → `sales_dt_window(...)`;
  `job` → `marketplace`; explicit decimal casts; `{{ source() }}` /
  `{{ ref() }}` references.
- **seaport**: drop the invalid `'2022-06-31'` bound (absorbed by the
  macro), the no-op `ORDER BY` inside the `sales` CTE, and the unused
  `contract_name` column from the `dim_contracts` join.
- **marketplace_v2** (amended during backfill — the accepted fan-out
  risk materialized): a multi-sale tx emits one fee/royalty MANA
  transfer per OrderSuccessful, each in a log preceding its order. The
  royalty join now requires `m.log_index < o.log_index` and keeps only
  the closest preceding transfer per order.
- **wyvern** (amended during backfill): an 1155 can move as several
  Transfer legs of the same token in one tx (e.g. qty 554 + qty 1);
  legs are pre-aggregated with SUM(quantity) per (tx, contract, token,
  from, to) before building the arrays, so UNNEST emits one position.
- **seaport matchOrders** (amended during backfill): a matched sale
  emits TWO OrderFulfilled halves — a listing knowing only the seller
  and a bid knowing only the buyer, adjacent logs in either order. The
  original `sc.buyer = sc.seller` guard never matched (NULLs), leaving
  two incomplete rows. Halves now pair on: same tx, adjacent logs
  (`ABS(diff) = 1`), opposite sides, same NFT, and both being
  incomplete (bid without seller, listing without buyer — a complete
  adjacent sale of the same NFT is a same-tx flip and stays separate).
  The bid row keeps the sale with parties COALESCEd across halves; the
  listing row is dropped. Paired totals stay net-to-seller, falling
  back to the listing side's total payments when the payout is routed
  through a proxy contract (no leg names the seller).

## Tests

Bounded to the processed window (PR #3 lesson — unbounded scans blow
the workgroup cap):

- `schema.yml`: `not_null` on `transaction_hash`, `log_index`,
  `block_timestamp`, `buyer`, `seller`, `nft_contract_address`,
  `token_id`, `quantity`, `currency`, `total_amount_raw`,
  `marketplace`, `chain_id`, `dt`; `accepted_values` on `marketplace`
  (the five values) and `chain_id` (`[1]`).
- `tests/assert_sales_unique_key.sql`: uniqueness of the declared
  grain over the last `sales_incremental_days` of partitions
  (via `$partitions`).
- `tests/assert_sales_amounts_sane.sql`: `total_amount_raw >= 0`,
  `royalty_amount_raw >= 0`, `royalty_amount_raw <=
  total_amount_raw`, same bounded window. Also the canary for
  float64-style price corruption (see incident
  `docs/incidents/2026-09-04-marketplace-v2-orders-float64.md`;
  staging is fixed and re-decoded).

## Known limitations

- **Stall on an empty window**: an N-day window with zero sales writes
  no partition and `max(dt)` never advances (shared with
  `nft_transfers`). More plausible here in the 2019–2021 gap where
  only v2/wyvern were active and volume was thin — catch-up runs with
  a large `sales_incremental_days` mitigate; revisit if it bites.
- **Global advance front**: a single-marketplace backfill cannot be
  expressed through the incremental mechanics; it is done by
  overwriting that marketplace's partitions (bounded full-refresh or
  manual `INSERT INTO` of the branch SELECT).
- **Older partitions are validated when loaded**, not on every run
  (bounded tests).
- **Scan cap vs window size** (learned during backfill): in the
  2021-2022 NFT boom a 180-day window exceeds the 1 GB workgroup cap
  (~26 MB/day of staging scanned); the catch-up used an adaptive
  window (halve on scan-cap failure down to 20/10/5/1, grow on empty
  windows). Backfilled 2018-08-30 → 2026-08-20 (staging's end — the
  decode Lambdas are not in the daily pipeline until phase 8).
