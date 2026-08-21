# Decentraland Data Platform — Design

**Date:** 2026-08-20 (updated 2026-08-21 with staging layer and decode Lambda)
**Status:** validated section by section in conversation; pending final document review

## 1. Goal

Data engineering portfolio project. A serverless pipeline on AWS that extracts
Decentraland on-chain data (raw contract logs on Ethereum and Polygon) and MANA
token prices, processes them through a medallion architecture
(bronze/staging/silver/gold/platinum), and serves them in a public dashboard.

Constraints: ~$0/month cost (AWS, GCP, and Streamlit free tiers), fully
reproducible from the repo (Terraform), and maximum interview value
(own ABI decoding, dbt, Step Functions, Terraform, CI/CD).

## 2. Data sources

- **On-chain**: BigQuery public datasets `crypto_ethereum` and `crypto_polygon`.
  Queries filtered by Decentraland contract addresses (Marketplace v1/v2,
  LANDRegistry, EstateRegistry, wearable collections, Bids, Polygon collection
  store) and by date partition. Logs arrive raw (hex topics/data) — decoding is
  ours. BigQuery permanent free tier: 1 TB of queries/month; the daily
  extraction consumes GBs.
- **Prices**: public CoinGecko API — daily MANA OHLCV/market data.

## 3. Medallion data lake (single S3 bucket)

| Layer | Content | Written by |
|---|---|---|
| `bronze/onchain_logs/chain=<c>/dt=<d>/` | raw hex logs, parquet | `extract_onchain` Lambda |
| `bronze/mana_prices/dt=<d>/` | near-raw CoinGecko response | `extract_prices` Lambda |
| `staging/decoded_events/chain=<c>/dt=<d>/` | decoded, normalized events | `decode` Lambda |
| `silver/` | `nft_sales`, `mana_prices_daily`, clean events | dbt |
| `gold/` | `daily_marketplace_kpis`, `asset_category_stats`, `mana_price_vs_activity` | dbt |
| `platinum/` | ultra-aggregated extracts (KBs), publicly readable | `export_platinum` Lambda |

A single bucket with prefixes (simpler for IAM and lifecycle). Glue Data
Catalog as metastore; everything queryable from Athena.

**Platinum** is the serving layer: the dashboard reads these parquet files over
HTTPS with no AWS credentials and no per-visit Athena queries. Rationale:
"gold is for analysts with SQL; platinum is the data contract with applications".

## 4. Python / SQL boundary

Key design decision (dbt-athena does not support Python models):

- **Python — `decode` Lambda (bronze → staging)**, record-level work:
  identify each event by `topics[0]`, decode topics/data with `eth_abi`
  (types declared per event), normalize units (wei→MANA, addresses,
  UTC timestamps).
- **SQL/dbt (staging → silver → gold)**, set-based work:
  deduplication by `(tx_hash, log_index)`, assembling the "sale" entity by
  joining events within the same transaction (`OrderSuccessful` + `Transfer`),
  unifying the sale paths (Marketplace, Bids, Polygon store) with UNION,
  category classification via seeds, USD price enrichment, filtering
  non-sales (mints, cancellations), data quality tests.

The user's hands-on experience decoding manually (hex substrings) shines in
`tests/test_decode.py`: manual decoding as an oracle against `eth_abi`, plus
validation of N sales against Etherscan/Polygonscan.

## 5. Reference data

dbt seeds (versioned CSVs in `dbt/seeds/`): contract addresses, asset
categories, event signatures. DynamoDB was ruled out: joins happen in Athena,
git versioning is superior for small reference data, and there is no
operational state to store (day-partition idempotency acts as the watermark).

## 6. Orchestration and operations

Step Functions (Standard, free tier), triggered by an EventBridge cron at
06:00 UTC:

```
[extract_onchain ∥ extract_prices] → decode → dbt build (silver→gold + tests) → export_platinum
```

- Declarative retries (2 attempts, exponential backoff) per step.
- Catch branch → SNS → email on failure.
- Failing dbt tests stop the pipeline before publishing platinum.
- Idempotency: rewriting the day's partition is safe.
- Backfill: same state machine with input `{"start_date": ..., "end_date": ...}`;
  defaults to "yesterday".
- dbt-core + dbt-athena as a Lambda container image (doesn't fit the 250 MB
  zip limit); estimated build 1-3 min, far below the 15-min Lambda limit.
  Documented scaling path: move that task to ECS Fargate if the build grows 10x.
- Least-privilege IAM per Lambda (extract writes only bronze; decode reads
  bronze and writes staging; dbt reads staging and writes silver/gold; export
  writes platinum). GCP service account key in SSM Parameter Store.
- CloudWatch logs with 7-day retention.

## 7. Consumption

Streamlit dashboard on Streamlit Community Cloud (free), reading `platinum/`
over HTTPS. Views: marketplace KPIs, MANA price vs activity, asset category
analysis, and an "About" page with the architecture diagram.

## 8. Infrastructure and CI/CD

- **Terraform** for everything (chosen over SAM for CV value and data-resource
  coverage); region us-east-1; never create resources through the console.
- **GitHub Actions**: on PRs `pytest` + `ruff` + `terraform validate/plan` +
  `dbt parse`; on main, image builds + `terraform apply` with OIDC
  (no stored access keys).

## 9. Testing

- **pytest**: extraction chunking/partitioning, decode logic (manual vs
  eth_abi with real log fixtures).
- **dbt tests**: uniqueness, not_null, freshness, price ranges in silver and gold.
- **Cross-validation**: decoded sales compared against block explorers.

## 10. Build order

1. Base Terraform (bucket, catalog, IAM) + prices Lambda → first data in bronze.
2. BigQuery Lambda → on-chain bronze.
3. Decode Lambda → staging.
4. dbt: silver + tests.
5. dbt: gold + platinum export.
6. Step Functions + EventBridge.
7. Dashboard + README with diagram.

## Out of scope (YAGNI)

Streaming/real-time, DynamoDB, Redshift, Airflow/MWAA, QuickSight, chains
beyond Ethereum and Polygon, a dedicated serving API.
