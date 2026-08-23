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
