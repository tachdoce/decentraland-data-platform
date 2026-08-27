# Marketplace Trades Decode (Ethereum, V3/V4 off-chain orders) — Design

**Date:** 2026-08-27
**Status:** Approved in conversation.

## Goal

Decode the `Traded` events of the Decentraland off-chain-orders
marketplaces (`MarketplaceV3`
`0x2d6b3508f9aca32d2550f92b2addba932e73c1ff` and `MarketplaceV4`
`0x1b67d0e31eeb6b52d8eeed71d3616c2f5b33b8e7`) already landed in
`bronze.ethereum_logs` into a new staging table
`staging.ethereum_marketplace_trades`, one row per trade, via a new
zip Lambda `decode-ethereum-marketplace-trades`. Same silhouette as
the **Seaport** decoder: the event payload is a nested dynamic struct,
so the Athena query only filters and fetches raw logs and all ABI
decoding happens in a pure-Python parser.

## Background

- Event: `Traded(address indexed _caller, bytes32 indexed _tradeId,
  Trade _trade)`, topic0
  `0xaaecdfa7e74e704650fcb273f630f42f68974eff42bfffc1732cf30db9e4685b`,
  emitted by both V3 and V4 (same signature).
- `Trade = (address signer, bytes signature, Checks checks,
  Asset[] sent, Asset[] received)`;
  `Asset = (uint256 assetType, address contractAddress, uint256 value,
  address beneficiary, bytes extra)`.
- `Checks` verified against real logs to have **7 static fields**
  (uses, expiration, effective, salt, contractSignatureIndex,
  signerSignatureIndex, plus an undocumented `uint256`) followed by
  `address[] allowed` and an external-checks array. Full ABI type
  string locked by decoding all sampled logs with eth_abi:

  ```
  (address,bytes,(uint256,uint256,uint256,bytes32,uint256,uint256,uint256,address[],(address,bytes4,uint256,bool)[]),(uint256,address,uint256,address,bytes)[],(uint256,address,uint256,address,bytes)[])
  ```

  The decoder ignores `signature` and `checks` (order metadata;
  expiration here is unix **seconds**, unlike V1/V2 — irrelevant since
  we do not extract it).
- `assetType`: 1 = ERC-20, 2 = **USD-pegged MANA** (price fixed in
  USD, settled in MANA), 3 = ERC-721, 4 = **collection item** (primary
  mint; `value` is the collection item id, not a token id).
- `sent` = what the signer gives; `received` = what the signer gets.
  A listing has NFTs in `sent` (signer is the seller); an accepted bid
  has NFTs in `received` (signer is the buyer). The caller is always
  the counterparty executing the trade on-chain.
- Semantics differ from Seaport in one welcome way: `caller` is always
  a real address (topic), so `buyer`/`seller` never go NULL.
- Activity starts ~2024-12 (V3 deployment); V4 follows later.
- Verified samples (dt 2025-05/2025-06, fixtures): listings of a
  CommunityContestCollection wearable, LAND, Estate; MANA payments;
  one log with non-empty per-asset `extra` bytes (dynamic lengths →
  SQL decode impossible, Python parser required).

## Decoding rules

- Query filters `address IN (V3, V4)` AND `topics[1] = <TRADED>` and
  fetches raw `topics`/`data` only (same shape as the Seaport query).
- Parser (`parse_traded`) walks the ABI layout word by word (no
  eth_abi at runtime; validated against eth_abi in tests): trade tuple
  offset → signer, `sent`/`received` array offsets → per-element
  offsets (elements are dynamic because of `extra`) → asset fields.
  `signature`, `checks` and `extra` are skipped.
- Route each asset by `assetType`: 3/4 → NFT arrays; 1/2 → payment
  arrays; any other value raises.
- `order_side`: `listing` if the NFTs sit in `sent`, `bid` if they sit
  in `received`, `unknown` otherwise (NFTs on both sides or neither).
- `buyer`/`seller`: `listing` → seller = signer, buyer = caller;
  `bid` → buyer = signer, seller = caller. Never NULL.
- Only sales reach staging (Seaport convention): rows with
  `order_side = 'unknown'` or an empty payment array are dropped in
  the handler with a logged count.
- `nft_token_ids` as decimal strings (`str(int)`, ≤78 chars); for
  `assetType` 4 the value is the collection item id — the
  `nft_asset_types` column lets silver tell them apart. Same for
  `payment_asset_types` (2 = USD-pegged amounts, 18 decimals but USD,
  not MANA wei).
- Beneficiaries: the per-asset `beneficiary` address, lowercase
  (`nft_beneficiaries` / `payment_beneficiaries`) — who actually
  receives each asset.
- Amount guardrail: any `payment_amounts` value ≥ 10^38 raises.
- Addresses lowercase at write time; `trade_id` kept as `0x` + 64 hex.
- Event `{start_date, end_date}` inclusive (shared `parse_event`),
  default UTC today−2; backfill = invoke per range.

## Table: `staging.ethereum_marketplace_trades`

Location `s3://<bucket>/staging/ethereum_marketplace_trades/`, parquet
+ snappy, partitioned by `dt` (registered per run by awswrangler;
`"$partitions"` keeps working). One row per `Traded` log; key
`(transaction_hash, log_index)`; append-only writes, downstream dbt
dedups keeping latest `decoded_at`.

| Column | Type | Notes |
|---|---|---|
| `transaction_hash` | string | |
| `log_index` | bigint | |
| `block_timestamp` | timestamp | |
| `marketplace_address` | string | which contract emitted (V3/V4) |
| `trade_id` | string | topics[3], `0x…` hash |
| `caller` | string | topics[2]; executes the trade |
| `signer` | string | signed the off-chain order |
| `order_side` | string | `listing` / `bid` |
| `buyer` | string | derived, never NULL |
| `seller` | string | derived, never NULL |
| `nft_contract_addresses` | array\<string\> | parallel nft arrays |
| `nft_token_ids` | array\<string\> | decimal-as-string; item id when type 4 |
| `nft_asset_types` | array\<bigint\> | 3 = ERC-721, 4 = collection item |
| `nft_beneficiaries` | array\<string\> | who receives each NFT |
| `payment_currencies` | array\<string\> | token contract (MANA) |
| `payment_amounts` | array\<decimal(38,0)\> | wei (or USD-pegged units when type 2) |
| `payment_asset_types` | array\<bigint\> | 1 = ERC-20, 2 = USD-pegged MANA |
| `payment_beneficiaries` | array\<string\> | who receives each payment |
| `bronze_extracted_at` | timestamp | lineage |
| `decoded_at` | timestamp | |
| `dt` | string | partition |

## Lambda: `decode-ethereum-marketplace-trades`

Mirror of `decode-ethereum-seaport-sales`: zip + AWSSDKPandas layer
(Python 3.13, ARM64), Athena UNLOAD read
(`athena-results/unload/marketplace_trades/<uuid>/`), timestamped
filename prefix, `wr.s3.to_parquet(mode="append")` with
`dtype={"payment_amounts": "array<decimal(38,0)>",
"nft_asset_types": "array<bigint>",
"payment_asset_types": "array<bigint>"}`.

### Modules (inside the existing `decode/` package)

- `decode/ethereum_marketplace_trades_query.py` — filter + raw fetch,
  date validation as the others.
- `decode/ethereum_marketplace_trades_parser.py` — pure function
  `parse_traded(topics, data) -> dict`: scalars + classified arrays +
  derived side/buyer/seller. No pandas, no AWS.
- `decode/ethereum_marketplace_trades_handler.py` — parse_event →
  UNLOAD read → apply parser, drop non-sales, guardrails, lowercase,
  `decoded_at` ms floor → append parquet.

## Terraform

- `terraform/lambda_decode_marketplace_trades.tf` — mirror of
  `lambda_decode_seaport.tf`: `archive_file` zip (`__init__.py`,
  `common.py`, the three modules), function, dedicated IAM role,
  CloudWatch log group, tags `component = "decode"`,
  `layer = "staging"`.
- `terraform/table_marketplace_trades.tf` — Glue table with the schema
  above (pattern of `table_seaport_sales.tf`).

## Testing

`tests/test_decode_ethereum_marketplace_trades_query.py`,
`tests/test_decode_ethereum_marketplace_trades_parser.py` and
`tests/test_decode_ethereum_marketplace_trades_handler.py` (same split
as the Seaport tests):

- Real logs embedded as fixtures
  (`tests/fixtures/marketplace_trades_2025.json`): the four sampled
  `Traded` logs (wearable, LAND, Estate listings; one with non-empty
  `extra`).
- Parser validated field-by-field against `eth_abi` decoding with the
  locked Trade ABI type string (dev dependency only).
- A synthetic **bid** trade built with `eth_abi.encode` (NFT in
  `received`) exercises the bid branch, since bronze samples were all
  listings.
- Unit tests: asset routing by type (incl. types 2 and 4), side and
  buyer/seller derivation, unknown-side and zero-payment skip in the
  handler, guardrail raise, token-id decimal-string parity, date
  validation.

## Out of scope (this iteration)

- Polygon V3/V4 marketplaces (same event; decoder reusable later).
- Decoding `checks`, `signature`, or per-asset `extra`.
- dbt silver model unifying all marketplace sales — separate design.
- Step Functions wiring.

## Workflow note

Developed on `feat/marketplace-trades-decode`, merged to `main` via PR
with squash merge.
