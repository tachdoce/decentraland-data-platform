# contracts-on-push: silver.dim_contracts + event-driven dbt run

**Date**: 2026-08-23
**Status**: validated with user
**Depends on**: `2026-08-21-nft-contracts-ingestion.md` (superseded names:
`nft_contracts` → `contracts`, renamed 2026-08-23), `2026-08-20-platform-architecture.md`

## 1. Goal

When the curated `contracts.csv` is pushed to `landing/contracts/`, the
pipeline must run end to end with no manual step: ingest the CSV to bronze
and, only if ingestion succeeds, build the silver models that depend on it.
Any failure (ingestion, dbt build, dbt tests) posts a red alert to Slack.

This delivers the first real silver model (`dim_contracts`), the project's
first container-image Lambda (`run-dbt`), and the second Step Functions
state machine (`contracts-on-push`).

## 2. Decisions (validated)

- **Single source of truth**: `bronze.contracts` (the curated CSV). The
  official Decentraland registry (`bronze.dcl_contracts`) is NOT merged
  into the dimension; its daily diff pipeline only alerts a human to
  update the CSV. `dim_contracts` therefore reads one source, no fusion.
- **`erc_type = -1` rows are excluded in silver.** Consumers of
  `dim_contracts` never see them. `erc_type = 0` (not yet classified,
  likely marketplaces) IS included.
- **Orchestration**: Step Functions (approach A), not Lambda Destinations,
  mirroring the `dcl-contracts-daily` pattern: Retry on transient Lambda
  errors, Catch → SNS custom contract → red Slack, Fail state.
- **dbt runs in AWS** as a container-image Lambda. Local `dbt build`
  remains available for development.
- **dbt selection**: `--select source:bronze.contracts+` — today that is
  `dim_contracts` + `dim_contracts_changes`; future models depending on
  the CSV join the run automatically without touching the state machine.

## 3. silver.dim_contracts (dbt model)

Grain: one row per `(chain_id, contract_address)`. Materialized: `view`
(project default for silver; data is small).

```sql
-- latest snapshot only: the dimension mirrors the CSV currently in force
where dt = (select max(dt) from bronze.contracts)
  and erc_type != -1
```

Columns: `chain_id`, `contract_address`, `contract_name`, `dcl_contract`,
`erc_type`, `first_mint_dt`, `extract_from_dt`, `dt as snapshot_dt`.

**Tests** (failing tests abort the run and alert):
- `not_null`: `chain_id`, `contract_address`, `dcl_contract`, `erc_type`.
- `accepted_values`: `chain_id` in {1, 137}; `erc_type` in
  {0, 20, 721, 1155} (-1 cannot appear post-filter).
- Singular test: duplicates by `(chain_id, contract_address)` (no
  dbt_utils dependency for a single check).
- Singular test: model is not empty (guards against a `max(dt)` scan over
  a table with no partitions silently yielding zero rows).

`dim_contracts_changes` keeps its current skeleton and contract
(change_type/chain_id/contract_address/dt, empty = no changes); its diff
logic is out of scope here.

## 4. run-dbt Lambda (container image)

First container-image Lambda of the project (dbt-core + dbt-athena do not
fit a zip; CLAUDE.md convention).

- **Image**: `public.ecr.aws/lambda/python:3.13` base, installs
  `dbt-core` + `dbt-athena` (pinned to the locally proven 1.12.x/1.11.x
  pair), copies the `dbt/` project — including the already-committed,
  secret-free `dbt/profiles.yml` (region, workgroup
  `decentraland-data-platform`, `s3_staging_dir`, schema `silver`) — into
  the image. Code only — no reference data baked in (convention). Athena
  credentials come from the Lambda role. The existing
  `generate_schema_name` macro already maps silver models to the plain
  `silver` schema.
- **Handler** (`dbt_runner/handler.py`): invokes dbt programmatically via
  `dbtRunner` (`from dbt.cli.main import dbtRunner`) with
  `build --select source:bronze.contracts+`. Event may override the
  selector (`{"select": "..."}`) for manual runs. On dbt failure (model or
  test) the handler raises, so Step Functions catches it.
- **Post-build INFO notification** (contract from the alerts design,
  2026-08-21): after a successful build the handler queries
  `silver.dim_contracts_changes`; if it returns rows, it publishes the
  custom contract to `decentraland-alerts` with `status = INFO`,
  `source = dbt`, `component = dbt build`, detail = change summary. Today
  the skeleton model always returns zero rows, so no INFO fires yet, but
  the contract is honored the day the diff logic lands.
- **Sizing**: memory 1024 MB, timeout 300 s (dbt parse + 2 Athena DDLs
  fit comfortably; Athena waits dominate).
- **Glue database `silver`**: created by Terraform (like `bronze`), so
  the Lambda role never needs `glue:CreateDatabase`.
- **IAM**: Athena start/get query in the tagged workgroup; Glue read on
  bronze + read/write (tables) on the silver database — dbt only creates
  views there, so no silver S3 data prefix is needed; S3 read
  `bronze/contracts/*` and read/write `athena-results/*`; `sns:Publish`
  on `decentraland-alerts`; basic logs. Log group with 7-day retention,
  standard tags (`component = "dbt", layer = "silver"`).
- **ECR**: one private repo `decentraland-run-dbt`, lifecycle policy
  keeping the last 3 images. Build/push is a documented two-command flow
  (`docker build` + `docker push` after `aws ecr get-login-password`);
  Terraform references the image by tag and redeploys on digest change.
- **Alarm**: added to `monitored_lambdas` (defense in depth: catches
  invocations outside the state machine).

## 5. contracts-on-push state machine

Trigger chain: S3 `ObjectCreated` on `landing/contracts/*.csv` →
EventBridge → `states:StartExecution`.

- The existing `aws_s3_bucket_notification.lake` gains `eventbridge =
  true` and DROPS the direct `lambda_function` block for
  `landing/contracts/` (otherwise ingestion would run twice). Other
  future landing triggers are unaffected.
- EventBridge rule: `source = aws.s3`, `detail-type = Object Created`,
  bucket = lake, `object.key` prefix `landing/contracts/` and suffix
  `.csv`. Target: the state machine, with an input transformer passing
  `{bucket, key}`.

States (same skeleton as `dcl-contracts-daily`):

1. `LoadContracts` — invokes `load-contracts`. The handler accepts the S3
   `Records` shape; the state builds that shape from `{bucket, key}` via
   Parameters, so the Lambda code does not change. Retry on
   `Lambda.ServiceException` / `TooManyRequests`; Catch → `NotifyFailure`.
2. `RunDbt` — invokes `run-dbt` with `{}` (default selector). Same
   Retry/Catch. `End = true`.
3. `NotifyFailure` — `sns:publish` with the custom contract
   (`source = step-functions`, `component = contracts`,
   `status = FAILED`, detail from `$.error`, execution URL). Next:
   `FailExecution` (Fail state).

No schedule: this machine is purely event-driven. The daily
`dcl-contracts-daily` machine is untouched.

## 6. Failure modes

| Failure | Behavior |
|---|---|
| CSV invalid (duplicates, bad erc_type, …) | `load-contracts` raises before writing; bronze untouched; Catch → red Slack (plus the existing Lambda-errors alarm) |
| dbt model error / test failure | `run-dbt` raises; silver may hold the prior view version (views are replaced atomically per model); Catch → red Slack |
| Transient Lambda throttle | Retry (2 attempts, backoff) before Catch |
| EventBridge rule mis-filter | Nothing runs; detectable because a CSV push produces no execution — checked in verification |

Idempotency: re-pushing the same CSV re-runs the chain and overwrites the
same `dt` partition and views — safe by design.

## 7. Testing & verification

- **pytest**: `dbt_runner/handler.py` unit-tested with a fake `dbtRunner`
  (success, failure→raise, selector override, INFO publish on non-empty
  changes via fake Athena/SNS clients). Existing `test_notify_slack.py`
  already covers the custom contract.
- **dbt**: `dbt parse` in CI; the new singular tests run in every build.
- **Manual E2E after deploy**: push `reference/contracts.csv` to
  `landing/contracts/`, watch the execution in the Step Functions
  console, then Athena-check `silver.dim_contracts` (expect 962 rows =
  971 − 9 excluded) and confirm no Slack alert. The red path is validated
  by invoking `run-dbt` directly with a selector that fails fast (e.g. a
  nonexistent selection raises), not by pushing broken files to landing/.

## 8. Out of scope (YAGNI)

- Diff logic for `dim_contracts_changes` (needs snapshot-over-snapshot
  comparison design; separate task).
- The phase-8 daily orchestration (decode → full dbt build → platinum);
  this machine only covers the CSV push path.
- CI/CD image builds (GitHub Actions OIDC docker push) — image is built
  and pushed locally for now; revisit when CI deploy lands.
- Gold models, platinum export.
