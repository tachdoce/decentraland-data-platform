# Silver NFT transfers — design

Date: 2026-08-28
Status: validated with the user (sections discussed and approved in chat)

## Goal

Promote `staging.ethereum_nft_transfers` (decoded ERC-721/1155 Transfer
events) into `silver.nft_transfers`: a clean, deduplicated, incrementally
built fact table that gold can aggregate (mints, holders, sales joins).

## Key decisions

1. **Not a plain copy of staging.** Silver adds exactly three things:
   dedup of double bronze extractions, the `chain_id` convention column,
   and bounded incremental loading. Everything else passes through.
2. **Unified cross-chain table.** Named `nft_transfers` (not
   `ethereum_nft_transfers`) with `chain_id = 1` hardcoded until the
   polygon decode lands; then the model becomes a UNION of both staging
   tables.
3. **Dedup per extraction run, not per row.** Bronze is append-only: a
   day extracted twice reaches staging twice under different
   `bronze_extracted_at` (the decode Lambda's `overwrite_partitions`
   removes decode re-runs, not bronze duplicates). A 1155 TransferBatch
   legitimately yields many rows per `(transaction_hash, log_index)` —
   even identical ones (same token_id twice in one batch) — so row-level
   dedup would corrupt data. Instead, keep only the rows of the latest
   run: `bronze_extracted_at = MAX(bronze_extracted_at) OVER (PARTITION
   BY dt, transaction_hash, log_index)`.
4. **Grain**: one row per transferred token position;
   `(chain_id, transaction_hash, log_index)` is NOT unique by design.
5. **No `erc_type` column** (user decision). Recover it when needed via
   `INNER JOIN silver.dim_contracts` on `(chain_id, contract_address)`.
6. **Double partition `(chain_id, dt)`**, matching the bronze S3 path
   convention `chain_id=1/dt=...`. `dt` stays `string`: ISO dates order
   lexicographically, awswrangler and every existing table use string,
   and partition projection (where types matter) is deliberately not
   used because it would break the `$partitions` trick.
7. **Incremental `insert_overwrite`** partitioned by `(chain_id, dt)`:
   re-running a day replaces its partitions — idempotent like the
   Lambdas.
8. **Bounded incremental advance, parameterized.** Each incremental run
   loads `(max(dt), max(dt) + N days]` where
   `N = var('nft_transfers_incremental_days', 10)`. Both bounds resolve
   zero-scan via `$partitions`: the existing `max_partition_dt` macro
   and a new `max_partition_dt_offset(relation, days)` macro. Catch-up
   uses `--vars '{nft_transfers_incremental_days: 180}'` (~16 runs from
   2019 to today) instead of ~280 runs at the default.
9. **Bootstrap branch seeds up to 2018-12-31.** On the first build
   (`is_incremental()` false — the table and its `$partitions` do not
   exist yet) the model loads `dt < '2019-01-01'` only (>1M rows beyond
   that); the rest arrives through capped incremental runs.
10. **Lineage columns**: `bronze_extracted_at` and `decoded_at` pass
    through from staging (never recomputed — they timestamp upstream
    events); `silver_processed_at = CAST(current_timestamp AS timestamp)`
    stamps this model's run.

## Model

`dbt/models/silver/nft_transfers.sql` (implemented):

- CTE `ranked`: staging columns + the window MAX of
  `bronze_extracted_at`; incremental/bootstrap WHERE lives here so the
  window only sees the loaded slice.
- Final SELECT: filters to the latest run, adds `silver_processed_at`,
  and ends with `chain_id, dt` — dbt-athena requires partition columns
  last, in `partitioned_by` order.
- New macro `dbt/macros/max_partition_dt_offset.sql`; new source block
  `staging.ethereum_nft_transfers` in `sources.yml`.

## Tests

- `not_null` on `transaction_hash`, `log_index`, `contract_address`,
  `token_id`, `quantity`, `from_address`, `to_address`, `chain_id`,
  `dt`; `accepted_values [1, 137]` on `chain_id`.
- Singular test `assert_nft_transfers_single_run.sql`: after dedup,
  every `(chain_id, transaction_hash, log_index)` must carry exactly one
  distinct `bronze_extracted_at`. This replaces the classic uniqueness
  test, which the 1155 batch grain makes impossible.

## Known limitations

- **Stall on an empty window**: if a full N-day window contains zero
  transfers, no partition is written and `max(dt)` never advances (same
  known issue as `ethereum_currency_transfers`). Unlikely on Ethereum
  NFT activity; revisit if it bites.
- **Cross-chain incremental advance is global.** With one chain this is
  moot; when polygon lands, decide between global `MAX(dt)` and
  per-chain advance — the model changes anyway (UNION).
- **Identical duplicate rows within one extraction run are
  undetectable** (same `bronze_extracted_at`, so the single-run test
  passes). This materialized during the 2026-08-28 backfill: a retried
  Athena INSERT left files from two queries in partition
  `dt=2021-08-23` (751 duplicate rows, silver > staging). Detection:
  compare per-dt row counts against staging; repair: delete the
  partition's S3 objects and re-run the model's SELECT for that day via
  `INSERT INTO`. Backfill verification should always include the
  per-dt count comparison.
- **The singular test is bounded to the loaded window**
  (`max(dt)` minus `nft_transfers_incremental_days`): an unbounded scan
  exceeded the 1 GB workgroup cap once the table passed ~half its full
  size. Older partitions were validated when loaded.
