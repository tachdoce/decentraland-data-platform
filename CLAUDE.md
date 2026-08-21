# Decentraland Data Platform

Data engineering portfolio project: a serverless AWS pipeline that extracts
Decentraland on-chain data (contract logs on Ethereum and Polygon) and MANA
prices, processes them through a medallion architecture, and serves them in a
public dashboard. Target budget: ~$0/month (free tiers).

## Architecture in 5 lines

- **Sources**: BigQuery public datasets (`crypto_ethereum`, `crypto_polygon`) for raw logs; `contracts.decentraland.org/addresses.json` for the contract registry (mainnet+matic only, daily full snapshot); CoinGecko for MANA prices.
- **Medallion lake in a single S3 bucket**: `bronze/` (raw) → `staging/` (decoded) → `silver/` (clean entities) → `gold/` (KPIs) → `platinum/` (public extracts for the dashboard).
- **Python/SQL boundary**: Python (`decode` Lambda with eth_abi) for record-level transformation; dbt-athena for everything set-based (dedup, joins, aggregations). dbt-athena does NOT support Python models.
- **Orchestration**: Step Functions — parallel extractions → decode → dbt build → platinum export. EventBridge daily cron 06:00 UTC. SNS for alerts.
- **Consumption**: Streamlit Community Cloud reading `platinum/` parquet over HTTPS (no live Athena, no credentials).

## Structure

| Folder | Role |
|---|---|
| `terraform/` | All infrastructure (S3, Lambdas, Step Functions, Glue/Athena, IAM, SNS) |
| `ingestion/onchain/` (common/ + one folder per chain), `ingestion/dcl_contracts/`, `ingestion/prices/` | Extraction Lambdas → bronze |
| `decode/` | Python Lambda bronze → staging (ABI decoding with eth_abi) |
| `reference/` | Hand-curated reference CSVs (chains, general marketplaces, categories) — consumed by dbt via seed-paths and copied into Lambda images |
| `dbt/` | dbt-athena project: staging → silver → gold (no local seeds/ — uses ../reference) |
| `platinum_export/` | Lambda gold → platinum |
| `dashboard/` | Streamlit app |
| `tests/` | pytest; `test_decode.py` validates eth_abi against manual decoding |
| `docs/superpowers/specs/` | Designs validated with the user |
| `docs/superpowers/plans/` | Implementation plans derived from each spec |

## Conventions and decisions

- **Language**: chat with the user in Spanish; ALL artifacts in English (code, variable/column/function names, specs, plans, commits, docs).
- Region: **us-east-1**. CLI user: `terraform-admin`.
- IaC with Terraform only (chosen over SAM); never create resources through the console.
- Never commit: tfstate, GCP keys, .tfvars with secrets (see .gitignore).
- Secrets (GCP service account) in SSM Parameter Store.
- Lambdas with heavy deps (BigQuery, eth_abi, dbt) ship as container images; light ones as zip.
- Partitions: `dt=YYYY-MM-DD` (daily), `month=YYYY-MM` (monthly). Never name a partition `date` (Athena reserved word).
- Chains by numeric EIP-155 `chain_id` (1=ethereum, 137=polygon) in columns AND S3 paths (`chain_id=1/dt=...`). Contract key is always `(chain_id, address)`; addresses lowercase at write time.
- Bucket: `decentraland-data-platform-${account_id}` (interpolated in Terraform, never hardcoded).
- Incremental, idempotent extraction by partition; backfill = same code with `{start_date, end_date}`. Small sources (dcl_contracts) use daily full snapshots instead.
- Reference data (hand-curated only) lives in `reference/` — DynamoDB deliberately ruled out. Data with a live official source enters as a pipeline source, not reference.
- Cost tags on everything: provider `default_tags` (project, managed_by) + per-resource `component` and `layer`; Athena spend via dedicated tagged workgroup. Tags must be activated in Billing console.
- Failing dbt tests stop the pipeline before publishing platinum.
- CI on GitHub Actions: pytest + ruff + terraform validate/plan + dbt parse on PRs; deploy with OIDC on main.

## Frequent commands

```bash
cd terraform && terraform plan     # ALWAYS read the plan before applying
cd terraform && terraform apply
aws lambda invoke --function-name <fn> --payload '{"date":"YYYY-MM-DD"}' --cli-binary-format raw-in-base64-out /dev/stdout
cd dbt && dbt build                # transforms and tests silver+gold
pytest tests/
```

## User context

Data engineer with ~5 years at The Sandbox; strong on data, learning
infra/IaC — explain Terraform resources and AWS concepts when introducing them.
Conversation language: Spanish.

## Workflow

New designs are validated section by section and stored in
`docs/superpowers/specs/`; each spec yields a plan in `docs/superpowers/plans/`
executed with checkpoints. Agreed build order: 1) base Terraform +
`extract_dcl_contracts` Lambda (addresses.json → bronze snapshot), 2) on-chain
Lambdas (read contract list from the S3 snapshot), 3) prices Lambda,
4) decode, 5) dbt silver, 6) gold+platinum, 7) Step Functions,
8) dashboard+README.
