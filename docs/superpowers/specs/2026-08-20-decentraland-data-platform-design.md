# Decentraland Data Platform — Design

**Date:** 2026-08-20 (updated 2026-08-21: staging layer, decode Lambda, dcl_contracts source, lake conventions, cost tagging)
**Status:** validated section by section in conversation; pending final document review

## 1. Goal

Data engineering portfolio project. A serverless pipeline on AWS that extracts
Decentraland on-chain data (raw contract logs on Ethereum and Polygon) and MANA
token prices, processes them through a medallion architecture
(bronze/staging/silver/gold/platinum), and serves them in a public dashboard.

Constraints: ~$0/month cost (AWS, GCP, and Streamlit free tiers), fully
reproducible from the repo (Terraform), and maximum interview value
(own ABI decoding, dbt, Step Functions, Terraform, CI/CD, FinOps tagging).

## 2. Data sources

- **On-chain logs**: BigQuery public datasets `crypto_ethereum` and
  `crypto_polygon`. Queries filtered by Decentraland contract addresses and by
  date partition. Logs arrive raw (hex topics/data) — decoding is ours.
  BigQuery permanent free tier: 1 TB of queries/month; daily extraction
  consumes GBs.
- **Decentraland contract registry**: the official
  `https://contracts.decentraland.org/addresses.json` endpoint, treated as a
  pipeline source (not repo reference data). Only `mainnet` and `matic`
  networks are extracted.
- **Prices**: public CoinGecko API — daily MANA OHLCV/market data.

## 3. Lake conventions

- **Partitions**: `dt=YYYY-MM-DD` (ISO) for daily tables, `month=YYYY-MM` for
  monthly tables. Distinct key names make granularity self-documenting.
  `date`/`timestamp` are avoided (Athena reserved words).
- **Chains**: identified by numeric EIP-155 `chain_id` (1 = Ethereum,
  137 = Polygon) in both model columns and S3 partition paths
  (`chain_id=1/dt=...`). Chosen so adding future chains is a data change, not
  a convention change. A `chains.csv` dimension in `reference/` maps
  `chain_id, chain_name, source_alias, native_token` (addresses.json calls
  Polygon `matic`).
- **Addresses**: always lowercase at write time; source formats mix
  checksum-case and lowercase. The natural key for any contract is
  `(chain_id, contract_address)` — the same address can be a different
  contract on another chain (verified: `0x480a...` is TechTribalMarc0matic on
  mainnet and MarketplaceV2 on matic).
- **Bucket**: single bucket
  `decentraland-data-platform-${account_id}` (name availability verified;
  account-id suffix guarantees global uniqueness and is interpolated in
  Terraform, never hardcoded).

## 4. Medallion data lake (single S3 bucket)

| Layer | Content | Written by | Partitioning |
|---|---|---|---|
| `bronze/onchain_logs/` | raw hex logs, parquet | `extract_onchain_*` Lambdas | `chain_id=<n>/dt=` |
| `bronze/dcl_contracts/` | daily full snapshot of addresses.json | `extract_dcl_contracts` Lambda | `dt=` (no chain: each snapshot holds all chains) |
| `bronze/mana_prices/` | near-raw CoinGecko response | `extract_prices` Lambda | `month=` (low volume; avoids tiny files) |
| `staging/decoded_events/` | decoded, normalized events | `decode` Lambda | `chain_id=<n>/dt=` |
| `silver/` | `nft_sales`, `mana_prices_daily`, `dcl_contracts_current`, `dcl_contracts_history` | dbt | per model |
| `gold/` | `daily_marketplace_kpis`, `asset_category_stats`, `mana_price_vs_activity` | dbt | per model |
| `platinum/` | ultra-aggregated public extracts (KBs) | `export_platinum` Lambda | none |

Glue Data Catalog as metastore; everything queryable from Athena through a
project-dedicated workgroup. **Platinum** is the serving layer: the dashboard
reads these parquet files over HTTPS with no AWS credentials and no per-visit
Athena queries.

## 5. The dcl_contracts source (Phase 1)

`extract_dcl_contracts` Lambda, daily via EventBridge:

1. GET `addresses.json`; fail explicitly on empty/malformed payloads (never
   write a broken snapshot).
2. Keep only `mainnet` and `matic`; map to `chain_id` 1/137.
3. Flatten to rows `chain_id INT, contract_address STRING (lowercase),
   contract_name STRING (trimmed — the source has `"CollectionManager "` with
   a trailing space)`.
4. Write one parquet per run to `bronze/dcl_contracts/dt=<run date>/`
   (~142 rows, KBs — full-snapshot pattern for small sources, vs the
   incremental pattern used for logs).

Silver derives `dcl_contracts_current` (latest snapshot) and
`dcl_contracts_history` (validity ranges — an SCD2 derived from snapshots).
`_DEPRECATED` name suffixes from the source become an `is_deprecated` flag in
silver; bronze keeps names verbatim.

**Dependency design**: the on-chain extraction Lambdas read the contract list
from the latest S3 snapshot — the only function touching the external endpoint
is this one. If the endpoint fails, the pipeline degrades gracefully to the
previous snapshot instead of breaking.

## 6. On-chain ingestion structure (one folder per chain, shared core)

```
ingestion/onchain/
├── common/            # engine written once: bigquery_client, s3_writer, extractor
├── ethereum/          # handler.py (thin entry) + config.py (chain_id=1, dataset=crypto_ethereum)
├── polygon/           # handler.py + config.py (chain_id=137, dataset=crypto_polygon)
├── Dockerfile         # one image; each Lambda uses a different handler
└── requirements.txt
```

One Lambda per chain (`extract-onchain-ethereum`, `extract-onchain-polygon`)
deployed from the same image via Terraform `for_each`. Adding a chain = new
config folder + one list entry. Step Functions runs chains as parallel
branches. Rationale: per-chain operational isolation (independent failures and
backfills; BigQuery datasets differ slightly) without code duplication.

## 7. Python / SQL boundary

dbt-athena does not support Python models, so:

- **Python — `decode` Lambda (bronze → staging)**, record-level work: identify
  each event by `topics[0]`, decode topics/data with `eth_abi` (types declared
  per event), normalize units (wei→MANA, addresses, UTC timestamps).
- **SQL/dbt (staging → silver → gold)**, set-based work: deduplication by
  `(chain_id, tx_hash, log_index)`, assembling the "sale" entity by joining
  events within the same transaction (`OrderSuccessful` + `Transfer`),
  unifying sale paths (Marketplace, Bids, CollectionStore) with UNION,
  category classification, USD enrichment, filtering non-sales, data quality
  tests.

**Token standard classification (ERC-721 vs ERC-1155)** is derived from event
signatures by **dominance, not presence**: contracts can emit stray
wrong-standard events (observed in production at The Sandbox: the 1155 ASSET
contract occasionally emitted 721-shaped Transfers). Raw counts per standard
are kept alongside the verdict, with a purity dbt test alerting on mixes above
threshold. Caveat encoded in silver: ERC-20 and ERC-721 share the same
`Transfer` topic0 and are distinguished by topic count (3 vs 4).

The user's manual hex-decoding experience becomes the test oracle:
`tests/test_decode.py` checks `eth_abi` output against hand-rolled substring
decoding, plus validation of sampled sales against block explorers.

## 8. Reference data

`reference/` at the repo root is the single source of truth for
**hand-curated** data only: `chains.csv`, general marketplace contracts
(OpenSea/Blur/etc — addresses to be verified on Etherscan before production
use), asset categories, event signatures. Consumers: dbt via
`seed-paths: ["../reference"]` (the `dbt/seeds/` folder is removed), and
Lambdas by copying `reference/` into their images at build time.

Data with a live official source (Decentraland's addresses.json) does NOT go
in `reference/` — it enters through the pipeline as a source (section 5).

## 9. Orchestration and operations

Step Functions (Standard, free tier), EventBridge cron 06:00 UTC:

```
[extract_dcl_contracts → [extract_onchain_ethereum ∥ extract_onchain_polygon] ∥ extract_prices]
   → decode → dbt build (silver→gold + tests) → export_platinum
```

- Declarative retries (2 attempts, exponential backoff) per step; Catch → SNS
  email on failure.
- Failing dbt tests stop the pipeline before publishing platinum.
- Idempotency: rewriting a partition is safe. Backfill: same state machine
  with `{"start_date", "end_date"}` input; defaults to "yesterday".
- dbt-core + dbt-athena as a Lambda container image; estimated build 1-3 min
  (15-min Lambda limit; documented scaling path: ECS Fargate).
- Least-privilege IAM per Lambda (each writes only its own prefix). GCP
  service account key in SSM Parameter Store.
- CloudWatch logs with 7-day retention.

## 10. Cost visibility (FinOps)

- Provider-level `default_tags`: `project`, `managed_by=terraform`.
  Per-resource tags: `component` (e.g. `ingestion-dcl-contracts`, `dbt`) and
  `layer` (bronze/staging/silver/gold/platinum/infra).
- Tags must be activated as cost allocation tags in the Billing console
  (manual, once; up to 24 h to appear in Cost Explorer).
- Athena spend is captured by tagging the project's dedicated **workgroup**
  (dbt's cost driver), which also sets `bytes_scanned_cutoff` as a cost
  safety net.

## 11. Consumption

Streamlit dashboard on Streamlit Community Cloud (free), reading `platinum/`
over HTTPS. Views: marketplace KPIs, MANA price vs activity, asset category
analysis, and an "About" page with the architecture diagram.

## 12. Infrastructure and CI/CD

- **Terraform** for everything (chosen over SAM); region us-east-1; never
  create resources through the console.
- **GitHub Actions**: on PRs `pytest` + `ruff` + `terraform validate/plan` +
  `dbt parse`; on main, image builds + `terraform apply` with OIDC.

## 13. Testing

- **pytest**: extraction chunking/partitioning; dcl_contracts flattening
  (fixtures from the real JSON, including the trailing-space key and duplicate
  cross-chain addresses); decode logic (manual vs eth_abi).
- **dbt tests**: uniqueness on `(chain_id, ...)` keys, not_null, freshness,
  price ranges, contract standard purity.
- **Cross-validation**: decoded sales compared against block explorers.

## 14. Build order

1. **Phase 1**: base Terraform (bucket, Glue database, Athena workgroup,
   tagging) + `extract_dcl_contracts` Lambda → first table in bronze.
2. On-chain Lambdas (BigQuery → bronze), reading contracts from the S3 snapshot.
3. Prices Lambda (CoinGecko → bronze).
4. Decode Lambda → staging.
5. dbt: silver + tests (including dcl_contracts current/history).
6. dbt: gold + platinum export.
7. Step Functions + EventBridge.
8. Dashboard + README with diagram.

## Out of scope (YAGNI)

Streaming/real-time, DynamoDB, Redshift, Airflow/MWAA, QuickSight, chains
beyond Ethereum and Polygon (though `chain_id` conventions leave the door
open), a dedicated serving API.
