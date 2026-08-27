# Legacy Marketplace Auctions Decode (Ethereum) — Design

**Date:** 2026-08-27
**Status:** Approved in conversation.

## Goal

Decode the three auction events of the Decentraland LegacyMarketplace
contract already landed in `bronze.ethereum_logs` into a new staging
table `staging.ethereum_legacy_marketplace_auctions`, one row per
event, via a new zip Lambda
`decode-ethereum-legacy-marketplace-auctions`. Same silhouette as the
Wyvern decoder: the whole decode fits in SQL (static word layout);
Python only does numeric conversions that exceed Athena types.

## Background

- Contract: `0xb3bca6f5052c7e24726b44da7403b56a8a1b98f8`
  (`LegacyMarketplace`, chain_id 1) — the original 2018 LAND
  marketplace.
- Events and topic0:
  - `AuctionCreated`
    `0x9493ae82b9872af74473effb9d302efba34e0df360a99cc5e577cd3f28e3cab2`
  - `AuctionCancelled`
    `0x88bd2ba46f3dc2567144331c35bd4c5ced3d547d8828638a152ddd9591c137a6`
  - `AuctionSuccessful`
    `0xedcc7e1c269bc295dc24e74dc46b129c8449e6b0544af73b57c4201b78d119db`
- Shared indexed topics: `topics[2]` = `assetId` (uint256, the NFT
  token id — for LAND the encoded coordinates, negative coords in
  two's complement), `topics[3]` = `seller`. Only `AuctionSuccessful`
  adds `topics[4]` = `winner` (the buyer).
- Data words (verified against real bronze logs, dt=2018-03-19):
  - word 0 (all three events): `id` — the bytes32 auction id hash.
    NOT a token id; the token id is `topics[2]`.
  - word 1 (`AuctionCreated`, `AuctionSuccessful`): `priceInWei` /
    `totalPrice` (MANA wei).
  - word 2 (`AuctionCreated` only): `expiresAt` — **unix milliseconds**
    (verified: sample values ≈ 1.52e12 → March 2018 only when divided
    by 1000; the legacy dApp sent JavaScript timestamps). Decoded as
    `expires_at = from_unixtime(value / 1000)`.
- The event does not carry the NFT contract address. Attributing which
  contract (LANDRegistry vs Estate, etc.) each `asset_id` belongs to
  is downstream silver work, not decode.

## Decoding rules

- Filter: `address = <LegacyMarketplace>` AND `topics[1]` in the three
  topic0 hashes. `event_name` derived from topic0 with a `CASE`.
- Data-length guards per event (words are static):
  `AuctionCreated` = 194 chars (`0x` + 3 words),
  `AuctionSuccessful` = 130 (2 words), `AuctionCancelled` = 66
  (1 word).
- `auction_id` kept as `0x`-prefixed 64-hex string (it is a hash, not
  a number).
- `asset_id`: `topics[2]` hex → unsigned decimal string in Python
  (`str(int(hex, 16))`), varchar up to 78 chars — same convention as
  `staging.ethereum_nft_transfers.token_id`, enabling joins without
  casts.
- `seller` / `winner`: last 40 hex chars of the topic, `0x`-prefixed,
  lowercase.
- `total_price`: hex → Python int → `Decimal`, stored as
  `decimal(38,0)`; values ≥ 10^38 raise (same guardrail as the other
  decoders). NULL for `AuctionCancelled`.
- `expires_at`: hex → int → ms → `timestamp(ms)` in Python. NULL for
  `AuctionCancelled` and `AuctionSuccessful`.
- Event `{start_date, end_date}` inclusive (shared `parse_event`),
  default UTC today−2; backfill = invoke per range.

## Table: `staging.ethereum_legacy_marketplace_auctions`

Location `s3://<bucket>/staging/ethereum_legacy_marketplace_auctions/`,
parquet + snappy, partitioned by `dt` (registered per run by
awswrangler; `"$partitions"` keeps working). One row per log; key
`(transaction_hash, log_index)`; append-only writes, downstream dbt
dedups keeping latest `decoded_at`.

| Column | Type | Notes |
|---|---|---|
| `transaction_hash` | string | |
| `log_index` | bigint | |
| `block_timestamp` | timestamp | |
| `event_name` | string | `AuctionCreated` / `AuctionCancelled` / `AuctionSuccessful` |
| `auction_id` | string | bytes32 hash, `0x…` |
| `asset_id` | string | token id, decimal-as-string (≤78 chars) |
| `seller` | string | `0x…`, lowercase |
| `winner` | string | nullable; only `AuctionSuccessful` |
| `total_price` | decimal(38,0) | wei; nullable (NULL on `AuctionCancelled`) |
| `expires_at` | timestamp | nullable; only `AuctionCreated`; from unix ms |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

## Lambda: `decode-ethereum-legacy-marketplace-auctions`

Mirror of `decode-ethereum-wyvern-sales`: zip + AWSSDKPandas layer
(Python 3.13, ARM64), Athena UNLOAD read
(`athena-results/unload/legacy_marketplace_auctions/<uuid>/`),
timestamped filename prefix, `wr.s3.to_parquet(mode="append")` with
`dtype={"total_price": "decimal(38,0)"}`.

### Modules (inside the existing `decode/` package)

- `decode/ethereum_legacy_marketplace_query.py` — builds the
  extraction query: filter + `CASE` for `event_name`, conditional
  substrings for the per-event words, date validation as the others.
- `decode/ethereum_legacy_marketplace_handler.py` — parse_event →
  UNLOAD read → postprocess (price hex→Decimal with guardrail,
  asset_id hex→decimal string, expires_at hex→timestamp, lowercase
  addresses, `decoded_at` ms floor) → append parquet.

## Terraform

- `terraform/lambda_decode_legacy_marketplace.tf` — mirror of
  `lambda_decode_wyvern.tf`: `archive_file` zip with explicit `source`
  blocks (`__init__.py`, `common.py`,
  `ethereum_legacy_marketplace_query.py`,
  `ethereum_legacy_marketplace_handler.py`), function, dedicated IAM
  role (Athena workgroup, Glue bronze read + staging table
  create/update, S3 bronze read + staging/athena-results read/write),
  CloudWatch log group, tags `component = "decode"`,
  `layer = "staging"`.
- `terraform/table_legacy_marketplace_auctions.tf` — Glue table with
  the schema above (pattern of `table_wyvern_sales.tf`).

## Testing

`tests/test_decode_ethereum_legacy_marketplace.py`:

- Real logs embedded as fixtures: at least one of each event type
  pulled from bronze.
- Query-produced decoding validated against `eth_abi` (dev dependency
  only), following the existing decode-test convention: fixtures run
  through the same substring logic the SQL applies, results compared
  with `eth_abi` decoding of the raw data + topics.
- Unit tests: event_name routing by topic0, nullable columns per
  event, asset_id decimal-string parity with `ethereum_nft_transfers`
  formatting (incl. a negative-coordinate LAND id), expires_at ms
  conversion, price guardrail raise, date validation.

## Out of scope (this iteration)

- NFT contract attribution for `asset_id` (silver, via heuristics /
  date ranges — the event has no contract address).
- dbt silver model unifying legacy auctions with the other
  marketplaces' sales.
- MarketplaceProxy / V2+ events (different signatures, carry
  `nftAddress`) — separate decoder later.
- Step Functions wiring.
