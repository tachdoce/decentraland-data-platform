# Decentraland Data Platform

Data engineering portfolio project: a serverless AWS pipeline that extracts
Decentraland on-chain data (contract logs on Ethereum and Polygon) and MANA
prices, processes them through a medallion architecture, and serves them in a
public dashboard. Target budget: ~$0/month (free tiers).

## Architecture in 5 lines

- **Sources**: BigQuery public datasets (`crypto_ethereum`, `crypto_polygon`) for raw logs; CoinGecko for MANA prices.
- **Medallion lake in a single S3 bucket**: `bronze/` (raw) → `staging/` (decoded) → `silver/` (clean entities) → `gold/` (KPIs) → `platinum/` (public extracts for the dashboard).
- **Python/SQL boundary**: Python (`decode` Lambda with eth_abi) for record-level transformation; dbt-athena for everything set-based (dedup, joins, aggregations). dbt-athena does NOT support Python models.
- **Orchestration**: Step Functions — parallel extractions → decode → dbt build → platinum export. EventBridge daily cron 06:00 UTC. SNS for alerts.
- **Consumption**: Streamlit Community Cloud reading `platinum/` parquet over HTTPS (no live Athena, no credentials).

## Structure

| Folder | Role |
|---|---|
| `terraform/` | All infrastructure (S3, Lambdas, Step Functions, Glue/Athena, IAM, SNS) |
| `ingestion/onchain/`, `ingestion/prices/` | Extraction Lambdas → bronze |
| `decode/` | Python Lambda bronze → staging (ABI decoding with eth_abi) |
| `dbt/` | dbt-athena project: staging → silver → gold. Reference data in `seeds/` |
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
- Incremental, idempotent extraction by `chain=/dt=` partition; backfill = same code with `{start_date, end_date}`.
- Reference data (contracts, categories) as dbt seeds — DynamoDB deliberately ruled out.
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
executed with checkpoints. Agreed build order: 1) base Terraform + prices
Lambda, 2) BigQuery Lambda → bronze, 3) decode, 4) dbt silver,
5) gold+platinum, 6) Step Functions, 7) dashboard+README.
