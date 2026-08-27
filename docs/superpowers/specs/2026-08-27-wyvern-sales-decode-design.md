# Wyvern Sales Decode (Ethereum) — Design

**Date:** 2026-08-27
**Status:** Approved in conversation (bounded change: mirrors the
Seaport decode pattern, spec kept short).

## Goal

Decode Wyvern `OrdersMatched` events (OpenSea 2018–mid-2022) from
`bronze.ethereum_logs` into `staging.ethereum_wyvern_sales` via a new
zip Lambda `decode-ethereum-wyvern-sales`. Bronze already holds the
historical logs (~10.3M Wyvern rows, 2017-01-01..today): the
whole-transaction extraction captured every Wyvern sale that moved a
curated NFT — no extra GCP download needed.

## Background

- Contracts: Wyvern v1 `0x7be8076f4ea4a4ad08075c2508e481d6c946d12b`,
  v2.3 `0x7f268357a8c2552623316e2562d90e642bb538e5`.
- Event: `OrdersMatched(bytes32 buyHash, bytes32 sellHash,
  address indexed maker, address indexed taker, uint256 price,
  bytes32 indexed metadata)`, topic0
  `0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9`.
- Data layout is fully static (3 words: buyHash, sellHash, price), so
  the decode lives in the Athena SQL (pattern of `decode/query.py`);
  Python only converts `price` hex → decimal(38,0) (exceeds bigint).
- Verified in bronze: Aug 2021 alone has 1,146,022 `OrdersMatched`.

## Table: `staging.ethereum_wyvern_sales`

Parquet + snappy at `s3://<bucket>/staging/ethereum_wyvern_sales/`,
partitioned by `dt`, append-only writes (same as seaport: re-runs add
rows; dbt dedups by `(transaction_hash, log_index)` keeping the latest
`decoded_at`). One row per `OrdersMatched`.

| Column | Type | Source |
|---|---|---|
| `transaction_hash` | string | bronze |
| `log_index` | bigint | bronze |
| `block_timestamp` | timestamp | bronze |
| `wyvern_address` | string | which version emitted |
| `buy_hash` | string | data word 0 |
| `sell_hash` | string | data word 1 |
| `maker` | string | indexed topic |
| `taker` | string | indexed topic |
| `price` | decimal(38,0) | data word 2 (wei; currency unknown here) |
| `metadata` | string | indexed topic (bytes32, lineage/debug) |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

Deliberately NOT in staging — the event does not carry them; dbt
resolves them in silver by joining `ethereum_nft_transfers` on
`transaction_hash`: NFT contract/token_id/quantity (transfers whose
from/to match maker/taker; bundles → arrays), buyer/seller (NFT leaving
the maker ⇒ listing, leaving the taker ⇒ accepted bid), currency
(matching ERC-20 transfer in the tx, else native ETH).

## Lambda: `decode-ethereum-wyvern-sales`

Same silhouette as the seaport Lambda: zip + AWSSDKPandas layer
(Python 3.13 ARM64), event `{start_date, end_date}` inclusive
(`WHERE dt BETWEEN '{start_date}' AND '{end_date}'`, default UTC
today−2), Athena UNLOAD read, append-only `to_parquet`, unique UNLOAD
scratch per run. Query filters `address IN (v1, v2.3)` AND topic0 AND
`cardinality(topics) = 4` AND `length(data) = 194` (3 words + 0x).
Amount guardrail: `price` ≥ 10^38 raises. Backfill = per-range invokes;
the 2021–2022 peak is the largest partition set in staging.

Modules: `decode/wyvern_query.py` (SQL builder with the full slicing),
`decode/wyvern_handler.py` (UNLOAD read → price/decoded_at postprocess
→ write). Terraform mirrors `lambda_decode_seaport.tf` +
`table_seaport_sales.tf`. Tests validate the SQL substr offsets against
eth_abi on a real fixture log and cover the handler postprocess.
