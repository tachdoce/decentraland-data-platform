# nft_contracts Ingestion (event-driven) — Component Spec

**Date:** 2026-08-21
**Status:** validated in conversation; pending implementation plan
**Follows:** `2026-08-20-platform-architecture.md` (conventions §3, layers §4,
ops §8, tagging §9, testing §12)

## Purpose

Turn the hand-curated `reference/nft_contracts.csv` into a versioned,
queryable bronze table via an event-driven flow: uploading the CSV to the
lake's landing prefix triggers a Lambda that validates it and writes a dated
parquet snapshot. The dictionary drives the on-chain extraction (which
contracts to filter, and from which date to backfill each one — the
`extract_from_dt` pattern the user ran in production at The Sandbox).

## Flow

```
reference/nft_contracts.csv (git = source of truth; publishing = upload)
   │  aws s3 cp ... s3://<bucket>/landing/nft_contracts/   (manual now, CI later)
   ▼
S3 Event Notification (ObjectCreated, prefix landing/nft_contracts/, suffix .csv)
   ▼
Lambda load_nft_contracts
   1. read the uploaded CSV
   2. VALIDATE — reject the whole file (raise, no partial write) on:
      wrong/missing header (must be exactly chain_id, contract_address,
      contract_name, first_mint_dt, extract_from_dt); chain_id not in the
      known set {1, 137}; address not 0x-prefixed lowercase hex;
      unparseable ISO dates; duplicate (chain_id, contract_address) pairs.
   3. write ONE parquet to bronze/nft_contracts/dt=<upload date UTC>/
   ▼
Glue table bronze.nft_contracts (no partition projection: the Lambda runs
ALTER TABLE ADD IF NOT EXISTS PARTITION after each snapshot, since updates
are infrequent and the catalog should list only real partitions)
   ▼
silver (later phase): nft_contracts_current (latest dt) + nft_contracts_history
```

Re-uploading the same day overwrites that day's partition (idempotent).
Snapshots make every curation change historized and queryable — same pattern
as `bronze.dcl_contracts`.

## Table: bronze.nft_contracts

| Column | Type | Rules |
|---|---|---|
| `chain_id` | INT | EIP-155; validated against known chains |
| `contract_address` | STRING | lowercase 0x hex |
| `contract_name` | STRING | may be empty (pending curation) |
| `first_mint_dt` | DATE | fact: first on-chain event. Sentinel `2001-01-01` = not yet computed |
| `extract_from_dt` | DATE | decision: extract data from this date onward. Default `2026-01-01` |
| `dt` | partition | upload date, `YYYY-MM-DD` |

`first_mint_dt` vs `extract_from_dt` is deliberate: one is a derivable fact,
the other an operational choice that bounds BigQuery scan cost and drives
per-contract backfill — the on-chain Lambda computes, per contract,
`expected range [extract_from_dt, yesterday] − existing partitions` and
fetches only the missing history.

## Downstream (documented here, built in later phases)

- `silver.nft_contracts_current` / `_history` (same derivation as
  dcl_contracts).
- **Unified contracts view**: `silver.contracts` merging
  `dcl_contracts_current` (official registry) and `nft_contracts_current`
  (curated), with a `source` column (`dcl_registry` | `curated`) — the single
  contract dictionary the rest of the pipeline joins against. Exact shape to
  be designed in the silver phase.
- The on-chain extraction reads contract lists and backfill ranges from the
  latest snapshots in S3 — the CSV is never baked into Lambda images.

## Infrastructure (Terraform, tagged `component=ingestion-nft-contracts`, `layer=bronze`)

- S3 Event Notification on the lake bucket: `ObjectCreated:*`, prefix
  `landing/nft_contracts/`, suffix `.csv` → Lambda (plus the matching
  `aws_lambda_permission` for s3.amazonaws.com scoped to the bucket ARN).
- Lambda `load-nft-contracts`: Python 3.13, zip + AWSSDKPandas layer (same
  packaging as extract-dcl-contracts), timeout 60 s, memory 512 MB.
- IAM: `s3:GetObject` on `landing/nft_contracts/*`, `s3:PutObject` on
  `bronze/nft_contracts/*`, basic logging; plus the minimum for the
  partition DDL: Athena start/get on the tagged workgroup, Glue
  get/create-partition on catalog+database+table, and read/write on
  `athena-results/*`.
- Glue table `bronze.nft_contracts`; partitions registered explicitly by
  the Lambda via Athena DDL in workgroup `decentraland-data-platform`.
- CloudWatch log group, 7-day retention.
- No EventBridge schedule — the S3 event IS the trigger.

## Repo layout

```
ingestion/nft_contracts/
├── handler.py          # S3-event entry: read → validate → parquet → bronze
├── validate.py         # pure validation/parsing functions (no AWS calls)
└── requirements.txt
reference/
├── nft_contracts.csv   # the curated source (830 rows)
└── README.md           # cut criterion, defaults semantics, publish command
tests/
└── test_nft_contracts.py
```

## Tests (pytest, fixture = the real reference/nft_contracts.csv)

1. Valid file parses to 830 rows with correct types.
2. Rejects: missing/renamed column, unknown chain_id, malformed address,
   bad date, duplicated (chain_id, address).
3. Empty contract_name is accepted (pending-curation rows).
4. Partition path `bronze/nft_contracts/dt=YYYY-MM-DD/`.
5. Parquet roundtrip: schema types (int32 chain_id, date32 dates), row count.

## Acceptance criteria

- Uploading `reference/nft_contracts.csv` to the landing prefix produces,
  within seconds and with no manual invocation, a parquet under
  `bronze/nft_contracts/dt=<today>/`.
- Athena: `SELECT count(*) FROM bronze.nft_contracts WHERE dt='<today>'`
  returns 830; `SELECT count(*) ... WHERE first_mint_dt > DATE '2001-01-01'`
  returns 0 (all sentinels, until computed).
- Uploading a CSV with a broken header logs an explicit validation error and
  writes nothing to bronze.
- Re-upload the same day: still exactly one object in the partition.
