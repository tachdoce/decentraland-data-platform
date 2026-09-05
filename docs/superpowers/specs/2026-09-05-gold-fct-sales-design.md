# gold.fct_sales — USD-priced sales fact

**Date:** 2026-09-05
**Status:** validated with the user (chat review, section by section)

## Goal

Publish marketplace sales as a gold fact table priced in USD, joining
`silver.sales` to the gold dimensions (`dim_nft_contracts`,
`dim_currency`) and to the daily quotes in `silver.token_prices`. The
fact feeds future gold KPIs and the platinum export for the dashboard.

No trigger for now: the model runs inside `dbt build`. Wiring it into a
Step Functions state machine is a later, separate task.

## Grain and keys

One row per token sold. Unique key:
`(chain_id, transaction_hash, log_index, sk_contract, token_id)` —
the silver grain with `nft_contract_address` replaced by `sk_contract`
(surrogate key from `dim_nft_contracts`, per the project convention).

## Schema

| Column | Type | Source |
|---|---|---|
| `transaction_hash` | varchar | silver.sales |
| `log_index` | int | silver.sales |
| `block_timestamp` | timestamp | silver.sales |
| `buyer` | varchar | silver.sales |
| `seller` | varchar | silver.sales |
| `sk_contract` | varchar | dim_nft_contracts |
| `token_id` | decimal | silver.sales |
| `quantity` | decimal | silver.sales |
| `currency_symbol` | varchar | dim_currency |
| `total_amount` | double | `total_amount_raw / 10^decimals`, ROUND 6 |
| `royalty_amount` | double | `COALESCE(royalty_amount_raw, 0) / 10^decimals`, ROUND 6 |
| `usd_total_amount` | double | `total_amount * price_usd` (sale-day quote), ROUND 6 |
| `usd_royalty_amount` | double | `royalty_amount * price_usd`, ROUND 6 |
| `chain_id` | int | silver.sales (1 for now) |
| `marketplace` | varchar | silver.sales |
| `silver_processed_at` | timestamp | silver lineage |
| `gold_processed_at` | timestamp | stamped at build time |
| `dt` | varchar | **partition** (dt only) |

Excluded on purpose: `bronze_extracted_at` and `decoded_at` (staging
lineage stops at silver; gold keeps only `silver_processed_at` +
`gold_processed_at`), and the raw addresses `nft_contract_address` /
`currency` (recoverable by joining the dims).

## Join semantics — INNER on purpose

All three joins are INNER:

- `dim_currency` on `(chain_id, currency)` — drops sales paid in
  non-curated tokens.
- `token_prices` on `(currency_symbol, dt)` — drops sales on days
  without a USD quote (`fetch_price = FALSE` tokens, or dt beyond
  `price_fill_end_dt`).
- `dim_nft_contracts` on `(chain_id, nft_contract_address)` — provides
  `sk_contract`; silver already restricts to curated contracts.

The user chose INNER over LEFT explicitly: `gold.fct_sales` contains
only sales with a curated payment token and a sale-day price.

## Price coverage — fact starts 2019-01-01

`silver.token_prices` starts 2019-01-01 (the DefiLlama backfill grid);
sales start 2018-08-30. The 2,854 sales of 2018 (MANA/ETH) have no
quote, so with INNER joins they can never reach gold. Decision
(validated): **accept the loss** — the fact starts 2019-01-01 and 2018
sales stay silver-only. Backfilling 2018 prices was considered and
rejected for now.

## Materialization and loading

- `materialized='incremental'`, `incremental_strategy='insert_overwrite'`,
  `partitioned_by=['dt']` (dt only; `marketplace` is a plain column —
  it buys no pruning in gold).
- Reuses the existing `sales_dt_window` macro, which needs one change:
  a `seed_end_dt` argument (default `'2019-01-01'`, preserving silver's
  behavior) so fct_sales can seed **January 2019** on the initial build
  (`sales_dt_window('2019-01-01', seed_end_dt='2019-02-01')`). Without
  it the seed window would be empty, the table would have zero
  partitions, and the `$partitions`-based incremental bound
  (`MAX(dt)`) would never engage.
- Incremental runs advance at most `sales_incremental_days` (default
  10) past the table's max dt — same var and same backfill recipe as
  silver.sales: repeated runs with
  `--vars '{sales_incremental_days: N}'` until reaching 2026-08-20
  (current silver/prices frontier).
- No dedup in gold: silver.sales already keeps one copy per grain
  (latest `decoded_at` + `bronze_extracted_at` per staging scan,
  insert_overwrite partitions) and its unique-key test guards it. The
  draft's `MAX(decoded_at)` window was dropped — besides being
  redundant it ignored `token_id`, which is part of the grain (wyvern
  unnests several tokens per log).
- The dt window is applied in a CTE over `silver.sales` *before* the
  joins: the macro emits an unqualified `dt`, ambiguous once
  `token_prices` (which also carries `dt`) joins in; the CTE also
  prunes partitions before joining.

## Testing

- `tests/assert_fct_sales_unique_key.sql`: grain uniqueness, bounded to
  the window the run just loaded via `max_partition_dt_offset` with
  `-sales_incremental_days` (nft_transfers lesson: unbounded scans blow
  the workgroup cap; old partitions were validated when loaded).
- `models/gold/schema.yml`: `not_null` on every column,
  `accepted_values` on `chain_id` ([1]).
- Seed verification: January 2019 must land 336 rows, matching
  `silver.sales` 1:1 for that window (every sale in it is MANA/ETH
  priced from day one) — verified during design exploration, along
  with a plausible USD volume (~$256k).

## Out of scope

- Step Functions / EventBridge trigger (user wires it later).
- The post-backfill "LEFT JOIN anti-duplicates" idea the user mentioned
  — revisit after the table is loaded to 2026-08-20.
- Polygon sales (silver.sales is Ethereum-only today).
- Platinum export of the fact.
