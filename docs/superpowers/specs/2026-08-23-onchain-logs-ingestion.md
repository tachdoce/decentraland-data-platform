# onchain-logs: BigQuery → bronze ethereum_logs / polygon_logs

**Date**: 2026-08-23
**Status**: validated with user
**Depends on**: `2026-08-20-platform-architecture.md` (conventions),
`2026-08-23-contracts-on-push-design.md` (provides `silver.dim_contracts`),
GCP access configured 2026-08-23 (service account `dcl-bq-reader@dcl-pipeline`,
key in SSM `/decentraland/gcp/bq-service-account-key`).

## 1. Goal

Phase 3 of the build order: extract raw Decentraland-related logs from the
BigQuery public blockchain datasets into bronze, one day and one chain per
invocation. This is the pipeline's core ingestion and the first component
that reads GCP from AWS.

Source tables (updates the architecture spec §2, which named the older
`crypto_*` datasets):

- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs`
- `bigquery-public-data.goog_blockchain_polygon_mainnet_us.logs`

## 2. Decisions (validated)

- **One multi-chain Lambda** `extract_onchain_logs` (container image:
  `google-cloud-bigquery` + `pyarrow` do not fit a zip). The chain is
  chosen per invocation, not per function. Amends architecture spec §7
  ("one Lambda per chain via for_each") for this component.
- **Payload**: `{"chain_id": 1 | 137, "date": "YYYY-MM-DD"}`.
  `chain_id` is REQUIRED; any other value fails with a clear error.
  `date` is optional; default = **UTC today − 2 days** (margin for the
  BigQuery partition to be complete).
- **One day per invocation.** Backfill is orchestrated externally by
  invoking N times (documented CLI loop; a Step Functions Map state can
  take over in phase 8). Amends the architecture-spec backfill note
  (`{start_date, end_date}` in one invocation) for this component.
  `extract_from_dt` in `dim_contracts` is informative input for deciding
  the backfill range — the Lambda does not filter by it.
- **Two bronze tables**, one per chain: `bronze.ethereum_logs` and
  `bronze.polygon_logs`. The chain is implicit in the table; paths carry
  only `dt=`.
- **Append-only partitions**: the parquet filename is the download
  timestamp, and the Lambda NEVER deletes existing objects. Re-running a
  day adds a new file to the same `dt=` partition. Consequence (contract
  with phase 5 decode): downstream readers of these bronze tables MUST
  deduplicate by keeping, per `dt` partition, only the rows with the
  latest `extracted_at`.
- **Query shape**: two-step — first collect the day's
  `transaction_hash`es that touched any Decentraland contract, then fetch
  **all** logs of those transactions (any emitting address,
  `ARRAY_LENGTH(topics) >= 1`). Rationale: a sale emits events from the
  marketplace, the NFT, and MANA in one transaction; capturing the whole
  transaction preserves the context decoding will need.
- **Contract list** comes from Athena at run time:
  `SELECT contract_address FROM silver.dim_contracts WHERE chain_id = ?`.
  An empty list is an error (something upstream is broken) — the Lambda
  fails instead of silently writing nothing.
- **The rendered BigQuery SQL is logged** to CloudWatch on every run, for
  debugging.
- **Sizing**: timeout 900 s, memory 2048 MB.

## 3. Lambda flow

1. Validate payload (`chain_id` ∈ {1, 137}; `date` ISO or absent).
2. Athena: fetch contract addresses for the chain from
   `silver.dim_contracts` (project workgroup). Empty → raise.
3. SSM: read the GCP key from
   `/decentraland/gcp/bq-service-account-key` (SecureString).
4. Record `extracted_at = now (UTC)`; this single value stamps the run
   (filename and column).
5. BigQuery (parameterized query — the date and the address array go in
   as query parameters, never string-interpolated; addresses are already
   lowercase in silver):

```sql
WITH tx AS (
  SELECT DISTINCT transaction_hash
  FROM `bigquery-public-data.<dataset>.logs`
  WHERE DATE(block_timestamp) = @dt
    AND address IN UNNEST(@addresses)
)
SELECT transaction_hash, log_index, block_timestamp, address, topics, data
FROM `bigquery-public-data.<dataset>.logs`
WHERE DATE(block_timestamp) = @dt
  AND ARRAY_LENGTH(topics) >= 1
  AND transaction_hash IN (SELECT transaction_hash FROM tx)
ORDER BY block_timestamp, log_index
```

   `<dataset>` from an internal map:
   `{1: "goog_blockchain_ethereum_mainnet_us", 137: "goog_blockchain_polygon_mainnet_us"}`.
   The rendered SQL and the parameter values are logged before running.
6. Write ONE parquet to
   `bronze/<ethereum|polygon>_logs/dt=<date>/<YYYY-MM-DD_HH-MM-SS>.parquet`
   (timestamp = `extracted_at`, dashes in the time part — colons in S3
   keys break URL handling and Windows downloads). Zero query results is
   a valid outcome (quiet day): write the empty parquet with the full
   schema so the partition exists and downstream never special-cases.
7. Log `total_bytes_processed` and the row count; return them in the
   response for orchestration visibility.

## 4. Bronze schema (both tables)

| Column | Type | Notes |
|---|---|---|
| `transaction_hash` | string | |
| `log_index` | int64 | |
| `block_timestamp` | timestamp (UTC) | |
| `address` | string | emitting contract, lowercase |
| `topics` | array<string> | length ≥ 1 guaranteed by the query |
| `data` | string | hex payload, decoded in phase 5 |
| `extracted_at` | timestamp (UTC) | download moment; dedup key downstream |

Partition: `dt` (string `YYYY-MM-DD`, projection-enabled Glue tables like
the existing bronze tables). `chain_id` is NOT a column here — it is
implied by the table and re-attached in silver.

## 5. BigQuery cost guardrails

The query scans the day's partition twice (CTE + main query). Rough
budget: ~2–4 GB/day Ethereum, more on Polygon; both chains daily fits the
1 TB/month free tier with headroom, but long backfills must be paced.

- `maximum_bytes_billed = 50 GB` per job — hard per-invocation ceiling.
- `total_bytes_processed` logged every run (CloudWatch is the meter).
- Large backfills: run spaced out across days/months of quota; a billed
  slot reservation is explicitly out of scope.

## 6. Infrastructure and IAM

- **ECR**: private repo `decentraland-extract-onchain-logs`, lifecycle
  keep-last-3, local build/push flow (same as `run-dbt`).
- **Lambda**: container image, Python 3.13 base, timeout 900 s, memory
  2048 MB, tags `component = "ingestion-onchain"`, `layer = "bronze"`.
  Log group, 7-day retention. Added to `monitored_lambdas`.
- **Glue**: tables `bronze.ethereum_logs` and `bronze.polygon_logs` with
  `dt` partition projection, created by Terraform.
- **IAM (least privilege, NO delete — append-only by design)**:
  - `ssm:GetParameter` on the GCP-key parameter ARN.
  - `s3:PutObject` on `bronze/ethereum_logs/*` and
    `bronze/polygon_logs/*` only.
  - Athena `StartQueryExecution`/`GetQueryExecution`/`GetQueryResults`
    on the tagged workgroup; Glue read on `silver.dim_contracts` (and
    its database/catalog); S3 read/write on `athena-results/*` — which
    also covers `dim_contracts` data: with no `s3_data_dir` configured,
    dbt-athena materializes tables under `s3_staging_dir`.
  - Basic CloudWatch logs.
- No EventBridge schedule yet — invocation is manual (CLI) until the
  phase-8 daily state machine takes over.

## 7. Failure modes

| Failure | Behavior |
|---|---|
| Invalid payload (bad/missing chain_id, malformed date) | Raise immediately, nothing external touched |
| `dim_contracts` empty for the chain | Raise — upstream breakage must be loud |
| SSM parameter missing / GCP auth fails | Raise; logged clearly |
| BigQuery over `maximum_bytes_billed` | Job aborted by BigQuery, Lambda raises; no partial writes (single-file write) |
| Zero logs for the day | Valid: empty parquet with full schema |
| Re-run same day | New timestamped file appended; downstream dedups by latest `extracted_at` |

## 8. Testing & verification

- **pytest (TDD, pure logic — no mocked BigQuery/Athena calls in unit
  tests)**: payload validation, date default (UTC today − 2), dataset
  selection per chain, SQL rendering (parameters, dataset name), S3 key
  construction (table prefix, `dt=`, dash-formatted timestamp), parquet
  schema including `extracted_at`, empty-result handling.
- **Manual E2E after deploy** — the single agreed test invocation:

```bash
aws lambda invoke --function-name extract-onchain-logs \
  --payload '{"chain_id": 1, "date": "2026-08-19"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

  Then: CloudWatch shows the rendered SQL and bytes processed; Athena
  `SELECT count(*) FROM bronze.ethereum_logs WHERE dt = '2026-08-19'`
  returns rows; spot-check one transaction against a block explorer.

## 9. Out of scope (YAGNI)

- Polygon E2E validation (same code path; runs when backfill starts).
- Backfill orchestration (Step Functions Map) — phase 8.
- Filtering contracts by `extract_from_dt` inside the query.
- Decoding (phase 5), prices (phase 4).
- Compaction/cleanup of superseded files in append-only partitions.
