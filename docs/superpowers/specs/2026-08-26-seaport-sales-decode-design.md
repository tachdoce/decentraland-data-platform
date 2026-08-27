# Seaport Sales Decode (Ethereum) — Design

**Date:** 2026-08-26
**Status:** Approved section by section in conversation; pending final spec review.

## Goal

Decode OpenSea Seaport `OrderFulfilled` events already landed in
`bronze.ethereum_logs` into a new staging table
`staging.ethereum_seaport_sales`, one row per fulfilled order, via a new
zip Lambda `decode-ethereum-seaport-sales`. Staging decodes **all**
Seaport orders present in bronze; filtering to Decentraland collections
is business logic and happens downstream in dbt (silver).

## Background

- Seaport contracts (same address on every chain, CREATE2): 1.1
  `0x00000000006c3852cbef3e08e8df289169ede581`, 1.2
  `0x00000000000006c7676171937c444f6bde3d6282`, 1.3
  `0x0000000000000ad24e80fd803c6ac37206a45f15`, 1.4
  `0x00000000000001ad428e4906ae43d8f9852d0dd6`, 1.5
  `0x00000000000000adc04c56bf30ac9d3c0aaa14dd`, 1.6
  `0x0000000000000068f116a894984e2db1123eb395`.
- Event: `OrderFulfilled(bytes32 orderHash, address indexed offerer,
  address indexed zone, address recipient, SpentItem[] offer,
  ReceivedItem[] consideration)`, topic0
  `0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31`.
- `SpentItem = (uint8 itemType, address token, uint256 identifier,
  uint256 amount)`; `ReceivedItem` adds `address recipient`.
- `itemType`: 0 = native ETH, 1 = ERC-20, 2 = ERC-721, 3 = ERC-1155
  (criteria types 4/5 are resolved before emission and do not appear in
  fulfilled events).

### Order sides

The event does not label "sale item" vs "payment"; it only states what
each side delivered:

- **Listing** (seller created the order): `offer` = NFTs from the
  `offerer`; `consideration` = payments (price, marketplace fee,
  royalties), each with its own `recipient`.
- **Accepted bid** (buyer created the order): `offerer` is the buyer,
  `offer` = ERC-20 (typically WETH); the NFT appears in `consideration`
  with the buyer as its item-level recipient.

The decoder therefore classifies every item of both arrays by
`itemType` and never assumes which side holds the NFTs.

### Verified examples

Tx `0xd8c5ed0ca8394ec6dace758237697e50ec2b39eb16c53c0cb9e7743166899fcb`
(dt=2026-08-17, Seaport 1.6) turned out to be an NFT swap
(`matchOrders`): log_index 497 has NFTs on both sides (LAND #12302 +
5.2 WETH traded for three LANDs) and log_index 498 is the counterleg
(3 NFTs offered, empty consideration). Kept as the swap/skip test
fixture. Tx
`0x905ace0afc258861541738ad84becc4ddcb49ef1df7bbdd0abe265d6e5dc1286`
log_index 49 (same dt) is the plain-listing fixture (1 ERC-721 offered,
1 WETH payment).

### Non-sale orders are skipped

Only sales reach staging. The handler drops decoded orders where
`order_side = 'unknown'` (NFTs on both sides or on neither — swaps,
`matchOrders` counterlegs) or with an empty payment array (an order
that moves NFTs for nothing is not a sale), logging how many rows were
skipped. The parser itself still classifies everything.

## Decoding rules

- Route each item of `offer` + `consideration` by `itemType`:
  - 2 / 3 → NFT arrays (`nft_*`).
  - 0 / 1 → payment arrays (`payment_*`). Native ETH is stored with
    currency `0x0000000000000000000000000000000000000000`.
- `order_side`: `listing` if the NFTs sit in `offer`, `bid` if they sit
  in `consideration`.
- `buyer` / `seller` (derived, nullable):
  - `listing` → buyer = `recipient`, seller = `offerer`.
  - `bid` → buyer = `offerer`, seller = `recipient`.
  - If the event-level `recipient` is the zero address (`matchOrders`
    style fulfillments), the side that would map from it is NULL —
    never invented.
- NFT movement per item: item in `offer` → from = `offerer`, to =
  event `recipient`; item in `consideration` → from = event
  `recipient`, to = the item's own recipient. NULL positions when the
  event `recipient` is the zero address.
- The event has no per-payment payer, so no `payment_from` column
  exists: only amounts + recipients, as stated by the log. Interpreting
  recipients (net price vs marketplace fee vs royalty) is dbt/silver
  work.
- Orders may pay several recipients in several currencies;
  `payment_currencies` is per-payment, so mixed-currency orders need no
  special casing.
- Addresses lowercase at write time. `nft_token_ids` decoded exactly
  like `staging.ethereum_nft_transfers.token_id`: `str(int(hex, 16))`,
  full decimal number as string (LAND ids use the high 128 bits and do
  not fit numeric types), enabling joins without casts.
- Amount guardrail: any `payment_amounts` / `nft_quantities` value
  ≥ 10^38 raises (same rule as the transfers Lambda) — fail loudly
  rather than truncate. 10^38 wei ≈ 10^20 ETH, unreachable in practice.

## Table: `staging.ethereum_seaport_sales`

Location `s3://<bucket>/staging/ethereum_seaport_sales/`, parquet +
snappy, partitioned by `dt` (registered per run by awswrangler; no
partition projection, so `"$partitions"` keeps working). One row per
`OrderFulfilled` log; key `(transaction_hash, log_index)`.

| Column | Type | Notes |
|---|---|---|
| `transaction_hash` | string | |
| `log_index` | bigint | |
| `block_timestamp` | timestamp | |
| `seaport_address` | string | which Seaport version emitted |
| `order_hash` | string | dedup/debug |
| `offerer` | string | verbatim from the event |
| `recipient` | string | verbatim; may be `0x0…0` |
| `order_side` | string | `listing` / `bid` |
| `buyer` | string | derived, nullable |
| `seller` | string | derived, nullable |
| `nft_contract_addresses` | array\<string\> | parallel nft arrays, position i = same NFT |
| `nft_token_ids` | array\<string\> | decimal-as-string |
| `nft_quantities` | array\<decimal(38,0)\> | 1 for ERC-721 |
| `nft_froms` | array\<string\> | nullable positions |
| `nft_tos` | array\<string\> | nullable positions |
| `payment_currencies` | array\<string\> | parallel payment arrays; `0x0…0` = native ETH |
| `payment_amounts` | array\<decimal(38,0)\> | wei |
| `payment_recipients` | array\<string\> | |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

## Lambda: `decode-ethereum-seaport-sales`

Same silhouette as `decode-ethereum-nft-transfers`: zip + AWSSDKPandas
layer (Python 3.13, ARM64), event `{start_date, end_date}` inclusive
with default UTC today−2, append-only writes via
`wr.s3.to_parquet(mode="append")` (bronze-style: re-runs add rows;
downstream dbt dedups by `(transaction_hash, log_index)` keeping the
latest `decoded_at`), timestamped filename prefix, unique UNLOAD
scratch per run
(`athena-results/unload/seaport_sales/<uuid>/`). Backfill = invoke per
day/range in batches of 10.

Extraction query (Athena, via `wr.athena.read_sql_query`,
`unload_approach=True`) only filters and fetches raw columns:

```sql
SELECT transaction_hash, log_index, block_timestamp, address,
       topics, data, extracted_at, dt
FROM "bronze"."ethereum_logs"
WHERE dt BETWEEN '<start>' AND '<end>'
    AND topics[1] = '<ORDER_FULFILLED_TOPIC>'
```

No address filter: topic0 already identifies the event; a non-Seaport
clone emitting it would be harmless in staging.

All ABI decoding happens in Python (nested dynamic arrays are
unreadable in SQL). No eth_abi at runtime: `OrderFulfilled` data has a
regular layout parsed word by word (32 bytes) —

```
word 0: orderHash            word 2: offset of offer[]
word 1: recipient            word 3: offset of consideration[]
arrays: [count][elements]; SpentItem = 4 static words,
        ReceivedItem = 5 static words
```

### Modules (inside the existing `decode/` package)

- `decode/seaport_query.py` — builds the extraction query (date
  validation as in `query.py`).
- `decode/seaport_parser.py` — pure function: raw hex `data` + topics →
  dict with scalars and the classified arrays. No pandas, no AWS.
- `decode/seaport_handler.py` — parse_event → Athena UNLOAD read →
  apply parser, derive `order_side`/`buyer`/`seller`, guardrails,
  `decoded_at` (ms floor) → `to_parquet` with
  `dtype={"nft_quantities": "array<decimal(38,0)>",
  "payment_amounts": "array<decimal(38,0)>"}`.

## Terraform (`terraform/lambda_decode_seaport.tf`)

Mirror of `lambda_decode_nft.tf`:

- `archive_file` zip from explicit `source` blocks (preserves the
  `decode/` package layout): `__init__.py`, `seaport_query.py`,
  `seaport_parser.py`, `seaport_handler.py` only.
- `aws_lambda_function` `decode-ethereum-seaport-sales`, handler
  `decode.seaport_handler.handler`, same memory/timeout as the
  transfers Lambda, env `LAKE_BUCKET`, `ATHENA_WORKGROUP`.
- Dedicated IAM role, same shape: Athena workgroup, Glue (bronze read +
  staging `ethereum_seaport_sales` create/update), S3 (bronze read,
  staging + athena-results read/write).
- CloudWatch log group; tags `component = "decode"`,
  `layer = "staging"`.
- Glue table `staging.ethereum_seaport_sales` defined in Terraform with
  the schema above (pattern of `table_onchain_logs.tf`).

## Testing

`tests/test_decode_seaport.py`:

- Real logs embedded as fixtures: the two orders of the verified tx
  above (3-item and 1-item listings) plus at least one accepted bid.
- Manual parser validated against `eth_abi` decoding (dev dependency
  only), following the `test_decode.py` convention.
- Unit tests for item routing by `itemType`, `order_side`,
  `buyer`/`seller` derivation on both sides, zero-address `recipient`
  → NULLs, guardrail raise, token id / quantity decoding parity with
  `ethereum_nft_transfers` formatting.

## Out of scope (this iteration)

- Polygon Seaport logs (same decoder is reusable later; table would be
  `staging.polygon_seaport_sales`).
- dbt silver model over this table (DCL filtering, fee/royalty
  classification, buyer/seller analytics) — separate design.
- Step Functions wiring — arrives with the orchestration phase.
