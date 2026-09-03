# Reference data

Hand-curated reference files. Git is the source of truth; publishing a file
means uploading it to the lake's `landing/` prefix, which triggers its
ingestion Lambda (see spec `2026-08-21-nft-contracts-ingestion.md`).

## contracts.csv

Curated dictionary of contracts to track (971 rows): Decentraland's own
contracts plus top NFT collections.

- **Origin**: Decentraland registry (`dcl_contract = TRUE` rows) merged with
  the user's BigQuery exploration — yearly NFT collection ranking by USD
  sales volume (2019-2026, Ethereum + Polygon).
- **Cut criterion (NFT rows)**: contracts with > $5M USD volume in at least
  one year.
- **Cleaning applied**: deduplicated by `(chain_id, contract_address)`,
  addresses lowercased, names trimmed / quote- and comma-free / pure ASCII.
- **Columns**:
  - `chain_id` — numeric EIP-155 (1 = Ethereum, 137 = Polygon)
  - `contract_address` — lowercase
  - `contract_name` — cleaned display name; 12 rows pending curation (empty,
    listed last per chain; one known: `0x2953...4963` polygon = OpenSea
    Shared Storefront)
  - `first_mint_dt` — date of the contract's first on-chain event.
    **Default `2001-01-01` = sentinel for "not yet computed"** (predates any
    blockchain); to be populated from BigQuery `MIN(block_timestamp)`.
  - `extract_from_dt` — operational decision: extract data from this date
    onward. **Default `2026-01-01`** (cost-conservative start). Widening a
    contract's history = edit its row; the on-chain extraction Lambda
    backfills the missing range automatically.
  - `dcl_contract` — `TRUE`/`FALSE`: whether the contract belongs to
    Decentraland itself.
  - `erc_type` — ERC token standard: `20`, `721` or `1155`. `0` = not yet
    classified (likely marketplace or similar, still processed); `-1` =
    excluded from processing.

To publish after editing:

```bash
aws s3 cp reference/contracts.csv \
  s3://decentraland-data-platform-<account_id>/landing/contracts/
```

## erc20_tokens.csv

ERC-20 tokens (plus the native-ETH placeholder) seen as payment currency in
Decentraland marketplace trades. Symbols and names were resolved from each
contract address via CoinGecko's contract lookup (Ethplorer for delisted
tokens); decimals were verified against DefiLlama responses.

- `chain_id` — numeric EIP-155. All rows are 1 (mainnet) today.
- `contract_address` — token contract, lowercase.
  `0x0000000000000000000000000000000000000000` is the native-ETH
  placeholder used when trades are paid in ETH.
- `name` — human-readable token name.
- `fsym` — price-source ticker. Informative only and NOT unique — GALA v1
  (`0x15d4…`) and GALA v2 (`0xd1d2…`) share `GALA`. Joins always use
  `(chain_id, contract_address)`.
- `decimals` — ERC-20 decimals for raw-amount conversion
  (`amount = raw / 10^decimals`). Not always 18: USDC/USDT use 6; GALA,
  CUBE and STEPN GMT use 8.

To publish after editing:

```bash
aws s3 cp reference/erc20_tokens.csv \
  s3://decentraland-data-platform-<account_id>/landing/erc20_tokens/
```
