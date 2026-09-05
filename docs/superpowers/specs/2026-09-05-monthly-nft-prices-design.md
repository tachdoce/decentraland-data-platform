# gold.fct_monthly_nft_prices — monthly NFT price and volume per contract

**Date:** 2026-09-05
**Status:** validated with the user (chat review, section by section)

## Goal

Publish a monthly aggregate of NFT sales per contract on top of
`gold.fct_sales`: median unit price in USD plus volume metrics, one row
per `(month, sk_contract)`. It feeds dashboard charts of price
evolution by collection.

No trigger, on purpose: the model is NOT part of the daily pipeline or
any state machine. It runs only when invoked manually with an explicit
month parameter.

## Grain and schema

One row per `(month, sk_contract)`.

| Column | Type | Source |
|---|---|---|
| `sk_contract` | varchar | fct_sales |
| `sales_count` | bigint | COUNT(*) |
| `nft_quantity` | decimal | SUM(quantity) |
| `usd_total_amount` | double | ROUND(SUM(usd_total_amount), 2) |
| `usd_median_unit_price` | double | ROUND(approx_percentile(usd_total_amount / quantity, 0.5), 2) |
| `month` | varchar | **partition**, `YYYY-MM`, taken verbatim from the var |

The median is over the **unit** price (`usd_total_amount / quantity`),
chosen explicitly over the per-sale total: a bundle of N tokens must
not count as one expensive sale.

## Parameter — required `month` var

- Invocation: `dbt build --select fct_monthly_nft_prices --vars '{"month": "2020-01"}'`.
- The var is **mandatory and validated**: if absent, or not matching
  `^\d{4}-(0[1-9]|1[0-2])$`, the model fails at compile time with
  `exceptions.raise_compiler_error` and a clear message. No silent
  defaults, no garbage partitions from malformed input.
- The dt range is derived in SQL from the var, reading only the
  target month's partitions of `fct_sales`:
  `dt >= '<month>' || '-01' AND dt < CAST(date('<month>' || '-01') + interval '1' month AS varchar)`.

## Materialization and loading

- `materialized='incremental'`, `incremental_strategy='insert_overwrite'`,
  `partitioned_by=['month']`: each run replaces exactly the `month`
  partition named by the var — idempotent and safe to re-run.
- No `$partitions` bound and no auto-advance logic: the window is fully
  determined by the var, so the model needs none of the
  `sales_dt_window` machinery.
- Backfill = loop over months (2019-01 → last closed month) invoking
  the same command per month, same recipe as previous backfills.
  `fct_sales` starts 2019-01-01, so that is the natural floor.

## Not wired to the pipeline

- No EventBridge rule and no state machine changes.
- The automatic selectors are NOT untouched-by-luck: two of them use
  downstream graphs that reach `fct_sales`
  (`source:bronze.token_prices+`, `source:bronze.erc20_tokens+`) and
  would pull this model in and fail on the missing var. Fix (validated
  with the user): the model carries `tags=['manual']` and the `run-dbt`
  Lambda always appends `--exclude tag:manual` to every build.
- Since `--exclude` beats direct selection, the model runs **only via
  local dbt** (`dbt build --select fct_monthly_nft_prices --vars ...`),
  which matches how backfills are run anyway. No `vars` support is
  added to the Lambda (YAGNI).
- The var guard raises only under `{% if execute %}`: at parse time
  every model is rendered on every dbt invocation, and an unconditional
  raise would break the daily pipelines.

## Testing (`models/gold/schema.yml`)

All relational tests bounded to the partition just loaded
(`month = var('month')`), per the nft_transfers lesson on unbounded
scans:

- `not_null` on `sk_contract`, `month`, `usd_median_unit_price`.
- `dbt_utils.unique_combination_of_columns` on `(month, sk_contract)`.
- `dbt_utils.expression_is_true`: `usd_median_unit_price >= 0` and
  `nft_quantity > 0`.

## Out of scope

- Any trigger or scheduling (explicit user requirement).
- Platinum export of this table.
- Chain/marketplace breakdowns (fct_sales is Ethereum-only today; the
  grain can grow later if needed).
