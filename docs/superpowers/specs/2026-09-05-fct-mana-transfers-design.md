# gold.fct_mana_transfers — Design

**Date:** 2026-09-05
**Status:** Approved

## Purpose

MANA transfers fact for the gold star schema: MANA (ERC-20) movements on
Ethereum with the amount converted to whole tokens, ready for dashboard
consumption. MANA-only by design — no `usd_amount` (user decision); USD
valuation, if ever needed, happens where the fact is consumed.

## Grain

One row per `(chain_id, transaction_hash, log_index)` — unique, unlike
1155 batches (ERC-20 Transfer emits one row per log). Enforced with a
uniqueness test.

## Source

`silver.mana_transfers` only. No joins: the fact is single-token, so it
carries no `token_address`, no surrogate key, and no dimension lookup;
`decimals = 18` is a hardcoded, commented constant.

## No dedup in gold

Silver already guarantees one copy per grain (dual dedup on
`decoded_at` + `bronze_extracted_at`, insert_overwrite partitions, and a
uniqueness test on the grain). Gold trusts silver, same as
`fct_nft_transfers` and `fct_sales`.

## Amount conversion — exact, no floating point

```sql
CAST(t.amount_raw * DECIMAL '0.000000000000000001' AS decimal(30, 18)) AS amount
```

- Multiply, never `POW(10, 18)`: `POW` returns `double` (~15-16
  significant digits), losing wei-level precision before any cast.
- `DECIMAL '0.000000000000000001'` is `decimal(18,18)` and represents
  10^-18 exactly; decimal multiplication adds scales
  (`decimal(38,0) × decimal(18,18)` → scale 18, precision capped at 38),
  so every wei converts exactly.
- Plain decimal division is wrong too: Trino division keeps
  `max(s1, s2)` as the result scale, so dividing by an integer decimal
  performs integer division.
- The outer `CAST` to `decimal(30,18)` keeps scale 18 (no precision
  loss) and only narrows integer digits to 12 (up to 10^12 MANA). MANA
  total supply is ~2,193M (~2.2×10^9), ~456× below the limit; a value
  that did not fit would raise a query error, never truncate silently.
- Known asymmetry: `fct_sales` uses `ROUND(raw / POW(10, decimals), 6)`.
  Accepted — amounts are the essence of this fact; migrating `fct_sales`
  to the exact pattern is a possible future cleanup, not in scope.

## Filters

- `WHERE t.amount_raw > 0`: legal ERC-20 zero-value no-op transfers stay
  in silver but are dropped from gold, matching `fct_nft_transfers`.
- Mints and burns (zero address in `from_address` / `to_address`) are
  kept: they are real supply movements.

## Columns

| Column | Notes |
|---|---|
| `transaction_hash` | |
| `log_index` | |
| `block_timestamp` | |
| `from_address` | zero address = mint |
| `to_address` | zero address = burn |
| `amount` | `decimal(30,18)`, exact wei conversion |
| `chain_id` | 1 only until the Polygon decode lands |
| `silver_processed_at` | lineage from silver |
| `gold_processed_at` | `CAST(current_timestamp AS timestamp)` |
| `dt` | partition column, last |

`amount_raw`, `token_address` and bronze/decode metadata stay in silver.

## Materialization and incremental strategy

- `materialized='incremental'`, `incremental_strategy='insert_overwrite'`,
  `partitioned_by=['dt']` — dt only, consistent with the other gold
  facts; `chain_id` is a regular column.
- Incremental window inlined with `max_partition_dt(this)` /
  `max_partition_dt_offset(this, ...)`, reusing the **same var**
  `mana_transfers_incremental_days` (default 10) as silver so both
  advance at the same cadence.
- Initial build seeds `dt < '2019-01-01'` (data starts 2017-09-06).

## Tests (`gold/schema.yml`)

- `dbt_utils.unique_combination_of_columns` on
  `(transaction_hash, log_index)`.
- `not_null` on every column except the processing timestamps
  (`fct_sales` precedent).
- `accepted_values [1]` on `chain_id` (widen when Polygon lands).
- `dbt_utils.expression_is_true`: `amount > 0`.

## Backfill recipe

Initial `dbt build --select fct_mana_transfers` seeds 2017–2018, then
repeated runs with `--vars '{mana_transfers_incremental_days: 100}'`
(Athena caps INSERT at 100 partitions per query) until the incremental
front reaches silver's max dt (2026-08-20 at design time, ~28 runs).
Final verification: yearly reconciliation vs
`silver.mana_transfers WHERE amount_raw > 0` (expected diff 0) plus a
spot check that `SUM(amount)` per year equals
`SUM(amount_raw) * 10^-18` computed on silver.
