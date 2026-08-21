# Decentraland Data Platform — Architecture

**Date:** 2026-08-20 (updated 2026-08-21)
**Status:** living document — general rules and conventions for the whole
platform. Component-level designs live in their own dated specs in this folder
and must follow the rules here.

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
  `crypto_polygon`, filtered by Decentraland contract addresses and date
  partition. Logs arrive raw (hex topics/data) — decoding is ours. BigQuery
  permanent free tier (1 TB queries/month) covers daily extraction.
- **Decentraland contract registry**: the official
  `https://contracts.decentraland.org/addresses.json` endpoint, treated as a
  pipeline source (see spec `2026-08-21-dcl-contracts-extraction.md`).
- **Prices**: public CoinGecko API — daily MANA OHLCV/market data.

## 3. Lake conventions

- **Partitions**: `dt=YYYY-MM-DD` (ISO) for daily tables, `month=YYYY-MM` for
  monthly tables. Distinct key names make granularity self-documenting.
  `date`/`timestamp` are never used as column or partition names (Athena
  reserved words).
- **Chains**: identified by numeric EIP-155 `chain_id` (1 = Ethereum,
  137 = Polygon) in both model columns and S3 partition paths
  (`chain_id=1/dt=...`). Adding a chain is a data change, not a convention
  change. A `chains.csv` dimension in `reference/` maps
  `chain_id, chain_name, source_alias, native_token`.
- **Addresses**: always lowercase at write time. The natural key for any
  contract is `(chain_id, contract_address)` — the same address can be a
  different contract on another chain (verified real case: `0x480a...` is
  TechTribalMarc0matic on mainnet and MarketplaceV2 on matic).
- **Bucket**: single bucket `decentraland-data-platform-${account_id}`
  (account-id suffix guarantees global uniqueness; interpolated in Terraform,
  never hardcoded).
- **Extraction patterns**: incremental + idempotent by partition for
  high-volume sources (logs); daily full snapshot for small sources
  (contract registry). Backfill = same code with `{start_date, end_date}`.

## 4. Medallion layers (single S3 bucket)

| Layer | Role | Written by |
|---|---|---|
| `bronze/` | raw, immutable landings, verbatim from sources | ingestion Lambdas |
| `staging/` | record-level normalization/decoding | Python Lambdas |
| `silver/` | clean entities, dedup, joins, business rules | dbt |
| `gold/` | aggregated business metrics | dbt |
| `platinum/` | tiny public extracts served to applications | export Lambda |

Glue Data Catalog as metastore; everything queryable from Athena through a
project-dedicated workgroup. **Platinum** rationale: the dashboard reads
parquet over HTTPS with no AWS credentials and no per-visit Athena cost —
"gold is for analysts with SQL; platinum is the data contract with
applications".

## 5. Python / SQL boundary

dbt-athena does not support Python models. The rule:

- **Python (Lambdas)** for record-level transformation that needs libraries —
  ABI decoding, unit normalization, API flattening. Lands in bronze/staging.
- **SQL (dbt)** for set-based work — deduplication, entity assembly via joins,
  unions, enrichment, aggregation, and all data quality tests. Produces
  silver/gold.

Data quality rule derived from production experience (The Sandbox): when
classifying contracts from event signatures, decide by **dominance, not
presence** — contracts emit stray wrong-standard events. Keep raw counts
alongside verdicts and test purity.

## 6. Reference data

`reference/` at the repo root holds **hand-curated** data only (contract
dictionary, chains dimension, general marketplace contracts, asset
categories, event signatures). Git is the source of truth; **publishing** a
curated file means uploading it to the lake's `landing/<name>/` prefix,
where an S3 event notification triggers an ingestion Lambda that validates
it and writes a dated parquet snapshot to bronze — the same snapshot+`dt`
pattern as pipeline sources (first instance: `nft_contracts`, spec
`2026-08-21-nft-contracts-ingestion.md`). Downstream consumers (dbt models,
extraction Lambdas) read from the lake, never from files baked into images.
Tiny static dimensions with no snapshot value (e.g. `chains.csv`) may
instead be materialized as dbt seeds via `seed-paths: ["../reference"]`
(no local `dbt/seeds/`). Data with a live official source enters through
the pipeline as a source, never as repo reference.

## 7. Ingestion structure

One folder per source under `ingestion/`. Multi-chain sources use one folder
per chain with a shared core (`common/` engine + thin per-chain
`handler.py`/`config.py`), deployed as one Lambda per chain from a single
image via Terraform `for_each`. Rationale: per-chain operational isolation
(independent failures/backfills) without code duplication.

## 8. Orchestration and operations

Step Functions (Standard, free tier), EventBridge cron 06:00 UTC:

```
[extract_dcl_contracts → [extract_onchain_* in parallel] ∥ extract_prices]
   → decode → dbt build (silver→gold + tests) → export_platinum
```

- Declarative retries (2 attempts, exponential backoff) per step; Catch → SNS
  email on failure.
- Failing dbt tests stop the pipeline before publishing platinum.
- Rewriting a partition is always safe (idempotency requirement for every
  component).
- Heavy-dependency Lambdas (BigQuery client, eth_abi, dbt) ship as container
  images; light ones as zips. dbt runs inside a Lambda (build ≈ 1-3 min, far
  under the 15-min limit; documented scaling path: ECS Fargate).
- Least-privilege IAM per Lambda: each writes only its own S3 prefix. GCP
  service account key in SSM Parameter Store. CloudWatch logs, 7-day
  retention.

## 9. Cost visibility (FinOps)

- Provider-level `default_tags`: `project`, `managed_by=terraform`.
  Per-resource: `component` (e.g. `ingestion-dcl-contracts`, `dbt`) and
  `layer` (bronze/staging/silver/gold/platinum/infra).
- Tags must be activated as cost allocation tags in the Billing console
  (manual, once; up to 24 h to appear in Cost Explorer).
- Athena spend captured via the dedicated tagged workgroup, which also sets
  `bytes_scanned_cutoff` as a cost safety net.

## 10. Consumption

Streamlit dashboard on Streamlit Community Cloud (free), reading `platinum/`
over HTTPS. Views: marketplace KPIs, MANA price vs activity, asset category
analysis, and an "About" page with the architecture diagram.

## 11. Infrastructure and CI/CD

- **Terraform** for everything (chosen over SAM); region us-east-1; never
  create resources through the console.
- **GitHub Actions**: on PRs `pytest` + `ruff` + `terraform validate/plan` +
  `dbt parse`; on main, image builds + `terraform apply` with OIDC.

## 12. Testing strategy

- **pytest** for Lambda logic, with fixtures captured from real source
  payloads (including known data quirks).
- **dbt tests** for every silver/gold model: uniqueness on
  `(chain_id, ...)` keys, not_null, freshness, domain ranges, purity checks.
- **Cross-validation** against external ground truth (block explorers) for
  decoded data.

## 13. Build order

1. **Phase 1 — DONE**: base Terraform (bucket, Glue database, Athena
   workgroup, tagging) + `extract_dcl_contracts` Lambda → first table in
   bronze (spec: `2026-08-21-dcl-contracts-extraction.md`).
2. **Phase 2**: event-driven `nft_contracts` ingestion (landing/ upload →
   Lambda → bronze snapshot; spec `2026-08-21-nft-contracts-ingestion.md`).
3. On-chain Lambdas (BigQuery → bronze), reading contract lists and
   per-contract `extract_from_dt` backfill ranges from the S3 snapshots.
4. Prices Lambda (CoinGecko → bronze).
5. Decode Lambda → staging.
6. dbt: silver + tests (incl. contracts current/history and the unified
   `silver.contracts` view).
7. dbt: gold + platinum export.
8. Step Functions + EventBridge.
9. Dashboard + README with diagram.

Each phase gets its own component spec in this folder and its own
implementation plan in `docs/superpowers/plans/`.

## Out of scope (YAGNI)

Streaming/real-time, DynamoDB, Redshift, Airflow/MWAA, QuickSight, chains
beyond Ethereum and Polygon (though `chain_id` conventions leave the door
open), a dedicated serving API.
