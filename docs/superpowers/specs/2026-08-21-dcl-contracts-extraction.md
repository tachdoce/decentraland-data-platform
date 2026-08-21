# dcl_contracts Extraction — Component Spec (Phase 1)

**Date:** 2026-08-21
**Status:** validated in conversation; pending final review
**Follows:** `2026-08-20-platform-architecture.md` (conventions §3, layers §4,
operations §8, tagging §9, testing §12)

## Purpose

Daily snapshot of Decentraland's official contract registry
(`https://contracts.decentraland.org/addresses.json`) into the bronze layer,
so that (a) downstream extraction knows which contracts to filter without any
runtime dependency on the external endpoint, and (b) contract deployments are
queryable over time.

This is Phase 1 of the build: the simplest real Lambda, and the vehicle for
creating the base Terraform (bucket, Glue database, Athena workgroup, IAM,
tagging) that every later component reuses.

## Behavior

`extract_dcl_contracts` Lambda, invoked manually (Step Functions will
schedule it from phase 8):

1. **Extract**: GET `addresses.json`. The response contains 7 networks; only
   `mainnet` and `matic` are kept.
2. **Validate**: fail explicitly (raise, no partial write) if the payload is
   empty, is not valid JSON, is missing either required network, or a network
   contains zero contracts. A broken snapshot must never be written.
3. **Transform**: flatten `{network: {name: address}}` to rows; map network →
   `chain_id` (mainnet=1, matic=137).
4. **Load**: write ONE parquet file per run to
   `s3://decentraland-data-platform-<account_id>/bronze/dcl_contracts/dt=<run_date>/`.
   Rerunning a date overwrites its partition (idempotent). An optional `date`
   event field enables manual re-runs; default is the invocation date (UTC).

## Table: bronze.dcl_contracts

| Column | Type | Rules |
|---|---|---|
| `chain_id` | INT | 1 (mainnet), 137 (matic) |
| `contract_address` | STRING | lowercased at write time (source mixes checksum case) |
| `contract_name` | STRING | trimmed (source has `"CollectionManager "` with trailing space); kept otherwise verbatim, including `_DEPRECATED` suffixes |
| `dt` | partition | snapshot date, `YYYY-MM-DD` |

~142 rows per snapshot (108 mainnet + 34 matic as of 2026-08-21), KBs per
run. Registered in the Glue Catalog and queryable from Athena.

Known data quirks the implementation must preserve/handle:

- Duplicate addresses across chains (`0x480a...`, `0xb966...`) — rows are
  distinct because the key is `(chain_id, contract_address)`; never dedupe by
  address alone.
- `_DEPRECATED` suffixes stay verbatim in bronze; silver derives an
  `is_deprecated` flag later.

## Downstream (documented here, built in later phases)

- Silver: `dcl_contracts_current` (latest snapshot) and
  `dcl_contracts_history` (validity ranges — SCD2 derived from snapshots).
- On-chain extraction Lambdas read the latest snapshot from S3 to build their
  BigQuery filters.

## Infrastructure (Terraform, all tagged per architecture §9)

Base (shared by the whole platform, created in this phase):

- S3 bucket `decentraland-data-platform-${account_id}` (private, versioning
  off, lifecycle rules deferred).
- Glue database `bronze`; Glue table `dcl_contracts`; partitions registered
  explicitly by the Lambda via ALTER TABLE ADD IF NOT EXISTS PARTITION in
  workgroup `decentraland-data-platform` (same pattern as nft_contracts; no
  crawler/MSCK needed in steady state).
- Athena workgroup `decentraland-data-platform` with results prefix and
  `bytes_scanned_cutoff`.

Component:

- Lambda `extract-dcl-contracts`: Python 3.13, zip packaging if `pyarrow`
  fits the 250 MB limit (container image as fallback), timeout 60 s,
  memory 512 MB. Tags: `component=ingestion-dcl-contracts`, `layer=bronze`.
- IAM role: `s3:PutObject` on `bronze/dcl_contracts/*`, the Athena/Glue
  permissions for partition DDL, plus basic logging.
- No EventBridge trigger: orchestration arrives with Step Functions
  (phase 8); until then, manual invokes.
- CloudWatch log group, 7-day retention.

## Repo layout

```
ingestion/dcl_contracts/
├── handler.py          # lambda entry: orchestrates extract→validate→transform→load
├── transform.py        # pure functions: filter networks, flatten, normalize
└── requirements.txt    # requests, pyarrow
tests/
└── test_dcl_contracts.py
```

Transformation logic lives in pure functions (no AWS/network calls) so tests
run without mocks.

## Tests (pytest, fixture = real addresses.json captured 2026-08-21)

1. Flatten produces exactly the mainnet+matic rows with correct `chain_id`.
2. Addresses are lowercased; names are trimmed (the `"CollectionManager "`
   case).
3. Cross-chain duplicate addresses survive as distinct rows.
4. Other networks (sepolia, amoy, arbitrum, goerli, mumbai) are excluded.
5. Validation raises on: empty body, invalid JSON, missing `matic`, empty
   network object.
6. Partition path is built as `bronze/dcl_contracts/dt=YYYY-MM-DD/`.

## Acceptance criteria

- `terraform apply` from a clean state creates base + component with no
  manual console steps.
- Manual invocation writes the parquet; a second invocation the same day
  overwrites it (no duplicates).
- `SELECT chain_id, count(*) FROM bronze.dcl_contracts WHERE dt = '<today>'
  GROUP BY chain_id` in Athena returns 1→108, 137→34 (± source changes).
- All resources visible in Cost Explorer under
  `component=ingestion-dcl-contracts` once tags activate.
