# Currency Transfers Decode Lambda (Ethereum) — Design

**Date:** 2026-08-27
**Status:** Approved in conversation (bounded change: mirrors the
nft-transfers decode pattern, spec kept short).

## Goal

Move ERC-20 `Transfer` decoding from the dbt silver model
`silver.ethereum_currency_transfers` to a new zip Lambda
`decode-ethereum-currency-transfers` writing
`staging.ethereum_currency_transfers`, then delete the silver model.
Two motivations:

- **Consistency**: every other decoded entity (nft transfers, seaport
  sales, wyvern sales) is a decode Lambda writing `staging`; silver
  stays for dbt-built clean entities only.
- **Backfill ergonomics**: the dbt model appended fixed 20-day windows
  after its own `max(dt)` (macro-driven) and only reached
  `dt=2019-02-18` after many builds. The Lambda takes an explicit
  `{start_date, end_date}` range per invoke — no macro, no
  window-chaining.

## Background

- Event: `Transfer(address indexed from, address indexed to,
  uint256 value)`, topic0
  `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`,
  `cardinality(topics) = 3` (the ERC-721 Transfer shares topic0 but has
  4 topics).
- No join to `dim_contracts`: untracked payment tokens are kept on
  purpose for future sales parsing (same decision as the silver model).
- Overflow-era filter kept from the silver model: drop amounts ≥ 2^96
  (`substr(data, 3, 40) = '0…0'`) — bogus events from the 2018
  overflow-exploit era, not real economic activity. This also
  guarantees `amount_raw` fits decimal(38,0).
- Data layout is one static word, so the decode lives in the Athena SQL
  (pattern of `decode/query.py`); Python converts `amount_hex` →
  decimal(38,0) (uint256 exceeds bigint).

## Table: `staging.ethereum_currency_transfers`

Parquet + snappy at `s3://<bucket>/staging/ethereum_currency_transfers/`,
partitioned by `dt`, written with `mode="overwrite_partitions"` (same
as nft transfers: re-runs replace the touched days — idempotent, no
downstream dedup needed). Grain: one row per
`(transaction_hash, log_index)`.

| Column | Type | Source |
|---|---|---|
| `transaction_hash` | string | bronze |
| `log_index` | bigint | bronze |
| `block_timestamp` | timestamp | bronze |
| `token_address` | string | emitting contract (`address`, lowercase) |
| `from_address` | string | indexed topic 2 |
| `to_address` | string | indexed topic 3 |
| `amount_raw` | decimal(38,0) | data word 0 (token's smallest unit) |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

## Lambda: `decode-ethereum-currency-transfers`

Same silhouette as the nft-transfers Lambda: zip + AWSSDKPandas layer
(Python 3.13 ARM64), event `{start_date, end_date}` inclusive
(`WHERE dt BETWEEN '{start_date}' AND '{end_date}'`, default single day
UTC today−2), Athena UNLOAD read, `overwrite_partitions` write, unique
UNLOAD scratch per run. Query filters topic0 AND
`cardinality(topics) = 3` AND `length(data) = 66` (1 word + 0x) AND the
2^96 overflow filter. Amount guardrail in Python: ≥ 10^38 raises
(defense in depth; the SQL filter already caps at 2^96).

Modules: `decode/currency_query.py` (SQL builder),
`decode/currency_handler.py` (UNLOAD read → amount/decoded_at
postprocess → write). Terraform mirrors `lambda_decode_nft.tf` (IAM
includes the partition-delete actions `overwrite_partitions` needs) +
`table_staging_nft_transfers.tf`. Tests validate the SQL substr offsets
against eth_abi on a real fixture log and cover the handler
postprocess.

Backfill = per-range invokes 2017-09-06 (MANA deployment) → today,
windowed like the dbt model was (~20-day ranges) to respect the
workgroup bytes-scanned cap.

## Silver teardown (simple follow-up, no plan of its own)

After the Lambda is deployed and backfilled:

- Delete `dbt/models/silver/ethereum_currency_transfers.sql`, its block
  in `dbt/models/silver/schema.yml`, and
  `dbt/tests/assert_ethereum_currency_transfers_unique_key.sql`.
- `DROP TABLE silver.ethereum_currency_transfers` and delete
  `s3://<bucket>/silver/ethereum_currency_transfers/` (destructive —
  confirmed with the user at execution time; the partial 2017→2019 data
  is superseded by the staging backfill).
