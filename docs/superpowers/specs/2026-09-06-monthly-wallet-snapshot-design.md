# gold.fct_monthly_wallet_snapshot — monthly wallet state snapshot

**Date:** 2026-09-06
**Status:** validated with the user (chat review, question by question)

## Goal

Publish a monthly snapshot of the state of every wallet that ever
received a Decentraland NFT or MANA at least once: first-touch
timestamps per DCL contract family, MANA balance, NFT holdings flags
and estimated USD value of those holdings. It feeds cohort/segmentation
analysis of the Decentraland user base over time.

Like `fct_monthly_nft_prices`, the model is NOT part of the daily
pipeline or any state machine. It runs only when invoked manually with
an explicit month parameter.

## Grain and semantics

One row per `(month, wallet_address)`.

`month = 'YYYY-MM'` means the wallet state **at the start of that
month** (user decision): every metric is computed over transfers with
`dt < '<month>-01'`, and NFT valuations use `fct_monthly_nft_prices`
of the **previous** month (the last closed one at that point).

Population: wallets that appear as `to_address` at least once before
the cutoff in either

- `gold.fct_nft_transfers` restricted to `dcl_contract = TRUE`, or
- `gold.fct_mana_transfers` (MANA only by construction).

The zero address (`0x000…000`, mint/burn counterpart) is excluded with
a single `wallet_address <> '0x0…0'` filter in the final SELECT (user
decision: one filter at the end, not scattered per CTE). Burns still
subtract from the burner's balance; mints still credit the receiver.

## Schema

| Column | Type | Definition |
|---|---|---|
| `wallet_address` | varchar | population key |
| `land_owner_first_time` | timestamp | MIN(block_timestamp) receiving from `LANDProxy` |
| `dclregistrar_first_time` | timestamp | MIN receiving from `DCLRegistrar` (NAMEs) |
| `dcllaunchcollection_first_time` | timestamp | MIN receiving from `DCLLaunchCollection` |
| `dcl_others_first_time` | timestamp | MIN receiving from any other `dcl_contract` NFT |
| `mana_first_time` | timestamp | MIN(block_timestamp) receiving MANA |
| `mana_balance` | double | ROUND(SUM(in − out), 2); NULL when ≤ 0.001 (dust/zero kept distinguishable from an exact 0 holding) |
| `own_dcl_nfts` | boolean | holds ≥ 1 DCL NFT at cutoff (FALSE when none) |
| `own_other_nfts` | boolean | holds ≥ 1 curated non-DCL NFT at cutoff (FALSE when none) |
| `dcl_nft_value` | double | Σ holdings × previous-month `usd_median_unit_price`, DCL contracts; 0 when unpriced/none |
| `other_nft_value` | double | same for curated non-DCL contracts |
| `gold_processed_at` | timestamp | run timestamp |
| `month` | varchar | **partition**, `YYYY-MM`, taken verbatim from the var |

The special contracts are identified by `contract_name` via
`dim_nft_contracts` (`'LANDProxy'`, `'DCLRegistrar'`,
`'DCLLaunchCollection'`) — never by hardcoded `sk_contract` hashes
(user decision: readable and robust to surrogate-key regeneration).

## Holdings logic

- **ERC-721**: current owner = receiver of the latest transfer per
  `(sk_contract, token_id)` before the cutoff, resolved with
  `ROW_NUMBER() OVER (PARTITION BY sk_contract, token_id ORDER BY
  block_timestamp DESC, log_index DESC) = 1` (log_index breaks
  same-block ties, an improvement over the draft's MAX-timestamp
  window which double-counts same-block re-transfers).
- **ERC-1155**: net position = Σ quantity received − Σ quantity sent
  per `(wallet, sk_contract, token_id... aggregated to contract)`,
  keeping wallets with `HAVING SUM(quantity) > 0`.
- Valuation: LEFT JOIN to `fct_monthly_nft_prices` on
  `p.month = previous month AND p.sk_contract = holding.sk_contract`;
  unpriced holdings contribute 0 to value but still set the ownership
  flags.
- `erc_type` and `dcl_contract` come from `dim_nft_contracts` joins
  (no subquery-IN lists).

## Parameter — required `month` var

Identical contract to `fct_monthly_nft_prices`:

- Invocation: `dbt build --select fct_monthly_wallet_snapshot --vars '{"month": "2020-01"}'`.
- The var is mandatory and validated against
  `^\d{4}-(0[1-9]|1[0-2])$` with `exceptions.raise_compiler_error`
  under `{% if execute %}` (parse-time rendering by the daily
  pipelines must stay harmless).

## Materialization and loading

- `materialized='incremental'`,
  `incremental_strategy='insert_overwrite'`, `partitioned_by=['month']`,
  `tags=['manual']`: each run replaces exactly the partition named by
  the var; the `run-dbt` Lambda's `--exclude tag:manual` keeps it out
  of every automatic selection.
- Unlike the per-month aggregates, each run scans the **full history**
  of `fct_nft_transfers` and `fct_mana_transfers` up to the cutoff
  (cumulative snapshot) — acceptable at monthly cadence.
- Backfill = loop over months starting at **2020-01** (user decision),
  same per-month invocation.

## Testing (`models/gold/schema.yml`)

All tests bounded to the partition just loaded
(`month = var('month')`):

- `not_null` on `wallet_address`, `month`.
- `dbt_utils.unique_combination_of_columns` on
  `(month, wallet_address)`.
- `dbt_utils.expression_is_true`: `mana_balance > 0.001` (only rows
  where it is not NULL survive the HAVING).

## Out of scope

- Any trigger or scheduling — monthly automation (for this model and
  `fct_monthly_nft_prices` together) is a separate future task.
- Platinum export of this table.
- Polygon: upstream facts are Ethereum-only today; the logic is
  chain-agnostic and absorbs Polygon when the facts do.
