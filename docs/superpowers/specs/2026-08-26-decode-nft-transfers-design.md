# decode-nft-transfers Lambda (bronze -> staging) — design

Date: 2026-08-26
Status: validated with the user (sections discussed and approved in chat;
v2 — the first implementation used UNLOAD + hand-rolled parquet/Glue code
in a container image; it was torn down and redesigned around
awswrangler + zip + managed layer at the user's request)
Scope: the staging Lambda only. The dbt model `silver.ethereum_nft_transfers`
(staging -> silver) gets its own spec/plan later.

## Goal

Decode ERC-721 and ERC-1155 Transfer events for curated NFT contracts from
`bronze.ethereum_logs` into `staging/ethereum_nft_transfers/` parquet,
with `token_id` as an exact decimal string (uint256 -> up to 78 digits).

## Why a Lambda (and not pure SQL)

Athena/Trino has no integer wider than 64 bits and no decimal wider than
38 digits, while LAND token ids encode the `x` coordinate in the high 128
bits of the uint256 — the exact hex -> decimal-string conversion is not
reasonably expressible in SQL. Python ints are arbitrary precision
(`str(int(h, 16))`), so this lands on the Python side of the project's
Python/SQL boundary: Athena does the set-based extraction, the Lambda does
the record-level conversion.

## Flow (awswrangler)

The Lambda uses **AWS SDK for Pandas (awswrangler)** end to end:

1. `wr.athena.read_sql_query(sql, database="bronze",
   workgroup="decentraland-data-platform", unload_approach=True,
   s3_output=<athena-results scratch>)` -> DataFrame. The SQL joins
   `bronze.ethereum_logs` with `silver.dim_contracts` (`chain_id = 1`,
   topic0 matched by `erc_type`: 721 Transfer, 1155 TransferSingle /
   TransferBatch), flattens the three shapes (721 with `quantity = 1`,
   TransferBatch exploded one row per token id via a sequence join, max
   4000 ids per batch), and keeps `token_id` / `quantity` as raw 64-char
   hex. Bounded by `dt BETWEEN start_date AND end_date` (constants ->
   guaranteed partition pruning; 1 GB workgroup cap as safety net).
   `unload_approach=True` avoids CTAS temp tables (fewer Glue perms,
   parquet-fast reads).
2. **pandas postprocess** (pure function, unit-tested): `token_id` hex ->
   exact decimal string, `quantity` hex -> int (fail loudly if >=
   decimal(38,0)), add `decoded_at` (ms precision).
3. `wr.s3.to_parquet(df, path=s3://<bucket>/staging/ethereum_nft_transfers/,
   dataset=True, partition_cols=["dt"], mode="overwrite_partitions",
   database="staging", table="ethereum_nft_transfers",
   filename_prefix=<run timestamp>_, dtype={"quantity": "decimal(38,0)"})`
   — writes one parquet per touched dt, overwrites only those partitions
   (idempotency for free) and registers them in Glue (keeps the
   `"$partitions"` convention working). Files are named
   `<YYYY-MM-DD_HH-MM-SS>_<uuid>.parquet` (wrangler appends its own
   suffix; the timestamp prefix follows the bronze naming convention).

## Packaging: zip + managed layer (not a container image)

pandas/pyarrow normally force a container image (250 MB zip limit), but
AWS publishes managed Lambda layers for awswrangler
(`AWSSDKPandas-Python3xx-Arm64`) that bundle pandas + pyarrow within the
limits. The Lambda therefore ships as a **plain zip** (handler + query
builder only) plus the managed layer ARN — no Docker, no ECR, direct
dev loop (`terraform apply` re-zips on change). Runtime pinned to the
newest Python with a published arm64 layer in us-east-1 (resolved at
implementation time).

## Contract

- Payload: `{start_date?, end_date?}` (YYYY-MM-DD, inclusive). Default:
  single day UTC today-2, same as extract-onchain-logs.
- Idempotent per partition via `overwrite_partitions`.
- Backfill = invoking per range in batches (same recipe as the onchain
  extraction backfill).
- On failure: raise -> SNS alert through the existing alerts contract
  (wired when orchestration lands, phase 8).

## Staging schema (`staging.ethereum_nft_transfers`)

| column | type |
|---|---|
| transaction_hash | string |
| log_index | bigint |
| block_timestamp | timestamp |
| contract_address | string |
| erc_type | int (721 or 1155) |
| token_id | string (exact decimal, up to 78 digits) |
| quantity | decimal(38,0) |
| from_address | string |
| to_address | string |
| bronze_extracted_at | timestamp |
| decoded_at | timestamp |
| dt | string, partition (YYYY-MM-DD) |

Grain: one row per (transaction_hash, log_index, token_id) — a
TransferBatch log yields one row per token id.

## Infrastructure (Terraform)

- New Glue database `staging` + table `staging.ethereum_nft_transfers`
  (parquet, dt partition key, no projection).
- Lambda `decode-nft-transfers`: zip via `archive_file`, arm64, managed
  AWSSDKPandas layer, 2048 MB / 300 s to start, tuned with real data.
- IAM: Athena on the workgroup; Glue read on bronze + silver; Glue
  read/write partitions on staging; S3 read `bronze/*` and `silver/*`,
  read/write `staging/*` and `athena-results/*` (wrangler scratch).
- Cost tags: `component = "decode"`, `layer = "staging"`.

## Known findings

- **Legacy LAND Transfer signatures** (found in the v1 smoke run): in
  2018 LAND emitted pre-standard (ERC-821 era) Transfer events
  (`0xd5c97f2e…`, `0x47c705b9…`, extra operator/userData params), NOT the
  standard ERC-721 topic. The current query misses early LAND history;
  supporting those signatures is the first improvement round. Recent-day
  data decodes fine (verified in v1: 2026-08-20 -> 5,566 transfers, 5,070
  ERC-721 + 496 ERC-1155, token ids up to 78 digits, exact cross-check
  against bronze).

## Open items (deliberately deferred)

- `silver.ethereum_nft_transfers` dbt model: next spec.
- Step Functions integration: phase 8.
- Polygon variant: mirrors this Lambda when its bronze is ready.
- Legacy LAND signatures: first query-improvement round.
