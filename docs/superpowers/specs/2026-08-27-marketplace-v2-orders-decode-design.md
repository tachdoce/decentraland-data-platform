# Marketplace V2 Orders Decode (Ethereum) — Design

**Date:** 2026-08-27
**Status:** Approved in conversation.

## Goal

Decode the three order events of the Decentraland Marketplace V2
(through its proxy `0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539`)
already landed in `bronze.ethereum_logs` into a new staging table
`staging.ethereum_marketplace_v2_orders`, one row per event, via a new
zip Lambda `decode-ethereum-marketplace-v2-orders`. Same silhouette as
the LegacyMarketplace decoder: the whole decode fits in SQL (static
per-event word layout); Python only does numeric conversions that
exceed Athena types.

## Background

- Contract: `0x8e5660b4ab70168b5a6feea0e0315cb49c8cd539`
  (`MarketplaceProxy`, chain_id 1) — the proxy that emits the events;
  the implementation (`Marketplace`
  `0x19a8ed4860007a66805782ed7e0bed4e44fc6717`) never appears as log
  address. "V2" distinguishes it from the LegacyMarketplace and from
  the later V3/V4 contracts.
- Events and topic0:
  - `OrderCreated`
    `0x84c66c3f7ba4b390e20e8e8233e2a516f3ce34a72749e4f12bd010dfba238039`
  - `OrderSuccessful`
    `0x695ec315e8a642a74d450a4505eeea53df699b47a7378c7d752e97d5b16eb9bb`
  - `OrderCancelled`
    `0x0325426328de5b91ae4ad8462ad4076de4bcaf4551e81556185cacde5a425c6b`
- Shared indexed topics: `topics[2]` = `assetId` (uint256 token id),
  `topics[3]` = `seller`. Only `OrderSuccessful` adds `topics[4]` =
  `buyer` (4 topics; the other two have 3).
- Data words (verified against real bronze logs, window
  2018-11-01..2019-01-31, a single length variant per event):
  - word 0 (all three): `id` — the bytes32 order id hash.
  - word 1 (all three): `nftAddress` — **unlike the legacy contract,
    the event carries the NFT contract address** (samples show
    LANDProxy `0xf87e…5d4d` and EstateProxy `0x959e…c297`).
  - word 2 (`OrderCreated`, `OrderSuccessful`): `priceInWei` /
    `totalPrice` (MANA wei).
  - word 3 (`OrderCreated` only): `expiresAt` — **unix milliseconds
    again** (verified: sample `0x168ea603480` ≈ 1.549e12 → Feb 2019
    only when divided by 1000; same JavaScript-timestamp quirk as the
    legacy dApp). Decoded as `from_unixtime(value / 1000)`.
- Data lengths: `OrderCreated` = 258 chars (`0x` + 4 words),
  `OrderSuccessful` = 194 (3 words), `OrderCancelled` = 130 (2 words).

## Decoding rules

- Filter: `address = <MarketplaceProxy>` AND `topics[1]` in the three
  topic0 hashes. `event_name` derived from topic0 with a `CASE`.
- Data-length guards per event: 258 / 194 / 130; topic cardinality
  guard: 4 for `OrderSuccessful`, 3 otherwise.
- `order_id` kept as `0x`-prefixed 64-hex string (a hash, not a
  number).
- `asset_id`: `topics[2]` hex → unsigned decimal string in Python
  (`str(int(hex, 16))`), varchar up to 78 chars — same convention as
  `staging.ethereum_nft_transfers.token_id`.
- `contract_address`: data word 1, last 40 hex chars, `0x`-prefixed,
  lowercase — joins directly against `silver.dim_contracts`.
- `seller` / `buyer`: last 40 hex chars of the topic, `0x`-prefixed,
  lowercase.
- `total_price`: hex → Python int → `Decimal`, stored as
  `decimal(38,0)`; values ≥ 10^38 raise (house guardrail). NULL for
  `OrderCancelled`.
- `expires_at`: hex → int → ms → `timestamp(ms)` in Python
  (`pd.to_datetime(unit="ms")`, raising loudly on garbage). NULL for
  `OrderCancelled` and `OrderSuccessful`.
- Event `{start_date, end_date}` inclusive (shared `parse_event`),
  default UTC today−2; backfill = invoke per range.

## Table: `staging.ethereum_marketplace_v2_orders`

Location `s3://<bucket>/staging/ethereum_marketplace_v2_orders/`,
parquet + snappy, partitioned by `dt` (registered per run by
awswrangler; `"$partitions"` keeps working). One row per log; key
`(transaction_hash, log_index)`; append-only writes, downstream dbt
dedups keeping latest `decoded_at`.

| Column | Type | Notes |
|---|---|---|
| `transaction_hash` | string | |
| `log_index` | bigint | |
| `block_timestamp` | timestamp | |
| `event_name` | string | `OrderCreated` / `OrderCancelled` / `OrderSuccessful` |
| `order_id` | string | bytes32 hash, `0x…` |
| `asset_id` | string | token id, decimal-as-string (≤78 chars) |
| `contract_address` | string | NFT contract, `0x…`, lowercase |
| `seller` | string | `0x…`, lowercase |
| `buyer` | string | nullable; only `OrderSuccessful` |
| `total_price` | decimal(38,0) | wei; nullable (NULL on `OrderCancelled`) |
| `expires_at` | timestamp | nullable; only `OrderCreated`; from unix ms |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

## Lambda: `decode-ethereum-marketplace-v2-orders`

Mirror of `decode-ethereum-legacy-marketplace-auctions`: zip +
AWSSDKPandas layer (Python 3.13, ARM64), Athena UNLOAD read
(`athena-results/unload/marketplace_v2_orders/<uuid>/`), timestamped
filename prefix, `wr.s3.to_parquet(mode="append")` with
`dtype={"total_price": "decimal(38,0)"}`.

### Modules (inside the existing `decode/` package)

- `decode/ethereum_marketplace_v2_query.py` — builds the extraction
  query: filter + `CASE` for `event_name`, conditional substrings for
  the per-event words, date validation as the others.
- `decode/ethereum_marketplace_v2_handler.py` — parse_event → UNLOAD
  read → postprocess (price hex→Decimal with guardrail, asset_id
  hex→decimal string, expires_at hex→timestamp, lowercase addresses,
  `decoded_at` ms floor) → append parquet.

## Terraform

- `terraform/lambda_decode_marketplace_v2.tf` — mirror of
  `lambda_decode_legacy_marketplace.tf`: `archive_file` zip with
  explicit `source` blocks (`__init__.py`, `common.py`,
  `ethereum_marketplace_v2_query.py`,
  `ethereum_marketplace_v2_handler.py`), function, dedicated IAM role,
  CloudWatch log group, tags `component = "decode"`,
  `layer = "staging"`.
- `terraform/table_marketplace_v2_orders.tf` — Glue table with the
  schema above (pattern of `table_legacy_marketplace_auctions.tf`).

## Testing

`tests/test_decode_ethereum_marketplace_v2_query.py` and
`tests/test_decode_ethereum_marketplace_v2_handler.py` (same split as
the legacy tests):

- Real logs embedded as fixtures
  (`tests/fixtures/marketplace_v2_orders_2018-11-01.json`): two per
  event type, pulled from bronze; they include LANDProxy and
  EstateProxy `nftAddress` values.
- Query-produced substr offsets validated against `eth_abi` (dev
  dependency only), same convention as the legacy tests.
- Unit tests: event_name routing by topic0, nullable columns per
  event, `contract_address` extraction + lowercase, asset_id
  decimal-string parity (incl. a negative-coordinate LAND id),
  expires_at ms conversion, price guardrail raise, date validation.

## Out of scope (this iteration)

- dbt silver model unifying V2 orders with legacy auctions and the
  other marketplaces' sales.
- MarketplaceV3 / V4 events — separate decoder later.
- Polygon marketplaces.
- Step Functions wiring.

## Workflow note

First feature under the new branch workflow: developed on
`feat/marketplace-v2-orders-decode`, merged to `main` via PR with
squash merge.
