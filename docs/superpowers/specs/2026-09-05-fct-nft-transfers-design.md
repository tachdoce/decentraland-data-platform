# gold.fct_nft_transfers — Design

**Date:** 2026-09-05
**Status:** Approved

## Purpose

NFT transfers fact for the gold star schema: transfers of curated NFT
contracts, keyed by `sk_contract`, ready for dashboard consumption. No
pricing involved — a transfer has no amount to value.

## Grain

One row per transferred token position, same as `silver.nft_transfers`:
`(chain_id, transaction_hash, log_index)` can legitimately repeat because
ERC-1155 `TransferBatch` events explode into one row per array element
(even with identical `token_id`). Therefore **no uniqueness test on that
grain** — silver doesn't carry one either.

## Source and joins

- `silver.nft_transfers` INNER JOIN `gold.dim_nft_contracts`
  ON `(chain_id, contract_address)`.
- Addresses travel via `sk_contract`; join the dim to recover
  `contract_address` / `contract_name`.
- The INNER JOIN is intentional: contracts outside `dim_nft_contracts`
  drop out of gold. In particular EstateProxy (`erc_type = 0`, prior user
  decision) is excluded, matching `fct_sales` behavior.

## No dedup in gold

Silver already guarantees one copy per extraction run
(`insert_overwrite` per partition plus its own dedup on
`bronze_extracted_at`). Gold trusts silver, same as `fct_sales`. The
draft's `MAX(silver_processed_at)` window is dropped: it would be a
no-op in practice and misrepresents the grain (it suggests
`(transaction_hash, log_index)` is unique, which 1155 batches violate).

## Columns

| Column | Notes |
|---|---|
| `transaction_hash` | |
| `log_index` | |
| `block_timestamp` | |
| `from_address` | |
| `to_address` | |
| `sk_contract` | from `dim_nft_contracts` |
| `token_id` | |
| `quantity` | |
| `chain_id` | 1 only until the Polygon decode lands |
| `silver_processed_at` | lineage from silver |
| `gold_processed_at` | `CAST(current_timestamp AS timestamp)` |
| `dt` | partition column, last |

Bronze/decode metadata (`bronze_extracted_at`, `decoded_at`) stays in
silver.

## Materialization and incremental strategy

- `materialized='incremental'`, `incremental_strategy='insert_overwrite'`,
  `partitioned_by=['dt']` — dt only, consistent with `fct_sales`;
  `chain_id` is a regular column, so one dt overwrite covers all chains
  when Polygon arrives.
- Incremental window inlined like `silver.nft_transfers` (not the
  sales-specific `sales_dt_window` macro), reusing the **same var**
  `nft_transfers_incremental_days` (default 10) with
  `max_partition_dt(this)` / `max_partition_dt_offset(this, ...)` so
  silver and gold advance at the same cadence during backfill.
- Initial build seeds `dt < '2019-01-01'` (full history from 2018 — no
  price dependency, unlike `fct_sales` which starts 2019).

## Tests (`gold/schema.yml`, fct_sales pattern)

- `not_null` on every column.
- `accepted_values [1]` on `chain_id` (widen when Polygon lands).
- `dbt_utils.expression_is_true`: `quantity > 0`.

## Backfill recipe

Same as `fct_sales`: initial `dbt build --select fct_nft_transfers`
seeds 2018, then repeated runs with
`--vars '{nft_transfers_incremental_days: N}'` until the incremental
front reaches silver's max dt (2026-08-20 at design time).
